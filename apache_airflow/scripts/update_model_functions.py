import json
from datetime import date
import pandas as pd
from datetime import datetime
import tensorflow as tf
from airflow.hooks.postgres_hook import PostgresHook

# add stats to fixtures table and save it in fixtures update table
def future_engineering():
    conn = None
    cur = None
    fixtures_df = None
    conflict_columns = ['fixture_id']
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    try:
        conn = None
        cur = None 
    
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        

        query = """
            SELECT *
            FROM fixtures
            WHERE fixture_status_short IN ('FT', 'WO', 'AET', 'PEN', 'CANC')
        """
        
        fixtures_df = pd.read_sql_query(query, conn)
        
        df = add_statistics(fixtures_df)
        
        update_columns = [col for col in df.columns if col not in conflict_columns]
        #insert data into tables

        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        insert_query = """
            INSERT INTO {} ({})
            VALUES ({})
            ON CONFLICT ({}) DO UPDATE SET {}
        """.format('fixtures_updated', ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns), update_set)

        cur.executemany(insert_query, df.values.tolist())
        print(f'table fixtures_updated updated')
        
        # Commit the changes
        conn.commit()
        return print('fixtures updated table up to date')
    except Exception as e:
        print(f'Error {e}')
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            # Close the cursor and connection
            cur.close()
        if cur is not None:
            conn.close()

def add_statistics(fixtures_df):
    
    fixtures_df['fixture_date'] = pd.to_datetime(fixtures_df['fixture_date']).dt.date
    fixtures_df = fixtures_df.sort_values(by='fixture_date')
    fixtures_df['teams_home_goals_scored_home'] = fixtures_df.groupby(['league_season', 'teams_home_id'])['goals_home'].cumsum()
    fixtures_df['teams_away_goals_scored_away'] = fixtures_df.groupby(['league_season','teams_away_id'])['goals_away'].cumsum()
    fixtures_df['teams_home_goals_lost_home'] = fixtures_df.groupby(['league_season','teams_home_id'])['goals_away'].cumsum()
    fixtures_df['teams_away_goals_lost_away'] = fixtures_df.groupby(['league_season','teams_away_id'])['goals_home'].cumsum()
    fixtures_df['teams_home_winner'] = fixtures_df.apply(
        lambda row: 3 if row['score_fulltime_home']>row['score_fulltime_away'] else (1 if row['score_fulltime_home']==row['score_fulltime_away'] else 0), axis=1
    )
    fixtures_df['teams_away_winner'] = fixtures_df.apply(
        lambda row: 3 if row['score_fulltime_home']<row['score_fulltime_away'] else (1 if row['score_fulltime_home']==row['score_fulltime_away'] else 0), axis=1
    )
    home = fixtures_df[[
        'fixture_date',
        'league_season',
        'teams_home_id', 
        'goals_home',
        'goals_away',
        'teams_home_winner', 
        'league_round'
        ]].rename(columns={
        'teams_home_id':'team_id',
        'goals_home':'goals_scored',
        'goals_away':'goals_lost',
        'teams_home_winner':'points'
        })
    away = fixtures_df[[
        'fixture_date', 
        'league_season',
        'teams_away_id', 
        'goals_away',
        'goals_home',
        'teams_away_winner', 
        'league_round'
        ]].rename(columns={
        'teams_away_id':'team_id', 
        'goals_away':'goals_scored',
        'goals_home':'goals_lost',
        'teams_away_winner':'points'
        })

    total = pd.concat([home, away])
    total = total.sort_values(by='fixture_date')
    total['total_goals_scored'] = total.groupby(['league_season','team_id'])['goals_scored'].cumsum()
    total['total_goals_lost'] = total.groupby(['league_season','team_id'])['goals_lost'].cumsum()
    total = total.sort_values(by='fixture_date')

    total['total_points'] = total.groupby(['league_season','team_id'])['points'].cumsum()

    total.sort_values(by=['league_season','league_round','total_points','total_goals_scored','fixture_date'], ascending=[True,True,False,False,True])
    total['standings'] = total.groupby(['league_season','league_round'])['total_points'].rank(method='min', ascending=False)
    total['standings'] = total['standings'].astype(int)

    total = total.sort_values(by=['team_id','fixture_date'])
    total['points_last_5_matches'] = total.groupby('team_id')['points'].rolling(window=5, min_periods=1).sum().reset_index(level=0, drop=True)
    total['points_last_5_matches'] = total['points_last_5_matches'].fillna(0)
    total['points_last_5_matches'] = total['points_last_5_matches'].astype(int)

    fixtures_df = fixtures_df.merge(total[[
        'fixture_date',
        'team_id',
        'total_goals_scored',
        'total_goals_lost', 
        'points', 
        'total_points', 
        'standings',
        'points_last_5_matches'
        ]], left_on = [
            'fixture_date',
            'teams_home_id'
            ],right_on = [
            'fixture_date',
            'team_id'
            ], how='left'
            ).rename(columns={
                'total_goals_scored':'teams_home_total_goals_scored',
                'total_goals_lost':'teams_home_total_goals_lost',
                'points':'teams_home_points',
                'total_points':'teams_home_total_points',
                'standings':'teams_home_standings',
                'points_last_5_matches':'teams_home_last_five_matches_points'
            }).drop(columns='team_id')
    
    fixtures_df = fixtures_df.merge(total[[
        'fixture_date',
        'team_id',
        'total_goals_scored',
        'total_goals_lost', 
        'points', 
        'total_points', 
        'standings',
        'points_last_5_matches'
        ]], left_on = [
            'fixture_date',
            'teams_away_id'
            ],right_on = [
            'fixture_date',
            'team_id'
            ], how='left'
            ).rename(columns={
                'total_goals_scored':'teams_away_total_goals_scored',
                'total_goals_lost':'teams_away_total_goals_lost',
                'points':'teams_away_points',
                'total_points':'teams_away_total_points',
                'standings':'teams_away_standings',
                'points_last_5_matches':'teams_away_last_five_matches_points'
            }).drop(columns='team_id')

    return fixtures_df

def get_matches(today, last_update):
    
    conn = None
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    #get next friday and monday dates as start and end for query
    
    # get upocoming matches playing from next friday to monday
    try:
        conn = pg_hook.get_conn()

        query = '''
    SELECT a.* 
    FROM (
        SELECT fu.*
        FROM fixtures_updated fu
        LEFT JOIN fixtures f ON fu.fixture_id = f.fixture_id
        WHERE fixture_date >= '{}' and fixture_date <= '{}' and fixture_status_short IN ('FT', 'WO', 'AET', 'PEN', 'CANC')
    ) a
    RIGHT JOIN predictions p ON a.fixture_id = p.fixture_id
    '''.format(last_update, today)
        query2 = '''
    SELECT p.*
    FROM fixtures f
    RIGHT JOIN predictions p ON f.fixture_id = p.fixture_id
    WHERE fixture_date >= '{}' and fixture_date <= '{}' and fixture_status_short IN ('FT', 'WO', 'AET', 'PEN', 'CANC')
    '''.format(last_update, today)
        matches = pd.read_sql_query(query, conn)
        predictions = pd.read_sql_query(query2, conn)
        print('got  matches')
        return matches, predictions
    except Exception as e:
        print(f'Error {e}')
        return None, None
    finally:
        if conn is not None:
            conn.close()

def add_stats(df):
    df['home_over_1'] = df.apply(lambda row: True if row['goals_home'] > 1 else False, axis=1)
    df['home_over_2'] = df.apply(lambda row: True if row['goals_home'] > 2 else False, axis=1)
    df['away_over_1'] = df.apply(lambda row: True if row['goals_away'] > 1 else False, axis=1)
    df['away_over_2'] = df.apply(lambda row: True if row['goals_away'] > 2 else False, axis=1)
    df['both_scores'] = df.apply(lambda row: True if (row['goals_home'] > 0)&(row['goals_away'] > 0) else False, axis=1)
    df['result'] = df.apply(lambda row: 0 if row['goals_home'] > row['goals_away'] else (1 if row['goals_home'] == row['goals_away'] else 2), axis=1)
    df['result_first_half'] = df.apply(lambda row: 0 if row['score_halftime_home'] > row['score_halftime_away'] else (1 if row['score_halftime_home'] == row['score_halftime_away'] else 2), axis=1)
    df['result_double_chance_home'] = df.apply(lambda row: 1 if row['goals_home'] >= row['goals_away'] else 0, axis=1)
    df['result_double_chance_away'] = df.apply(lambda row: 1 if row['goals_home'] <= row['goals_away'] else 0, axis=1)
    return df

def update_models():

    today = date.today()
    path = '/opt/airflow/data/last_update.json'
    with open(path, 'r') as f:
        last_update = json.load(f)
    date_format = '%Y-%m-%d'
    last_update = datetime.strptime(last_update, date_format).date()
    matches, predictions = get_matches(today, last_update)

    if len(matches) > 1000:
        matches_stats = add_stats(matches)
        
        # goal model
        goal_model = tf.keras.models.load_model('/opt/airflow/models/goal_model.h5')
        
        X = matches_stats[[
            'day_of_week', 
            'league_id', 
            'league_type_encoded', 
            'teams_home_id', 
            'teams_home_total_goals_scored',
            'teams_home_total_goals_lost', 
            'teams_home_last_five_matches_points',
            'teams_home_goals_scored_home', 
            'teams_home_goals_lost_home',
            'teams_away_id', 
            'teams_away_total_goals_scored',
            'teams_away_total_goals_lost', 
            'teams_away_last_five_matches_points',
            'teams_away_goals_scored_away', 
            'teams_away_goals_lost_away'
        ]]
        y_1 = matches_stats['home_over_1']
        y_2 = matches_stats['home_over_2']
        y_3 = matches_stats['away_over_1']
        y_4 = matches_stats['away_over_2']
        y_5 = matches_stats['both_scores']
        
        # Fine-tune the model using only the new data
        params = {
            'learning_rate': 0.001,
            'epochs': 10,
            'batch_size': 32,
            'validation_split': 0.2
        }
        
        params_list, metrics_list, updated_model = training(
            goal_model, 
            X, 
            y_1, 
            y_2, 
            y_3, 
            y_4, 
            y_5, 
            **params
        )
        updated_model.save('/opt/airflow/models/goal_model.h5')
        
        # result model
        X = matches_stats[[
            'day_of_week', 
            'league_id', 
            'league_type_encoded',
            'teams_home_id',
            'teams_home_total_goals_scored',
            'teams_home_total_goals_lost',
            'teams_home_last_five_matches_points',
            'teams_home_goals_scored_home',
            'teams_home_goals_lost_home',
            'teams_home_total_points',
            'teams_home_standings',
            'teams_away_id',
            'teams_away_total_goals_scored',
            'teams_away_total_goals_lost',
            'teams_away_last_five_matches_points',
            'teams_away_goals_scored_away',
            'teams_away_goals_lost_away',
            'teams_away_total_points',
            'teams_away_standings'
        ]]
        
        y_1 = matches_stats['result']
        y_2 = matches_stats['result_first_half']
        y_3 = matches_stats['result_double_chance_home']
        y_4 = matches_stats['result_double_chance_away']
        
        result_model = tf.keras.models.load_model('/opt/airflow/models/result_model.h5')
        
        # Fine-tune the model using only the new data
        params = {
            'learning_rate': 0.001,
            'epochs': 10,
            'batch_size': 32,
            'validation_split': 0.2
        }
        
        params_list, metrics_list, updated_model = training(
            result_model, 
            X, 
            y_1, 
            y_2, 
            y_3, 
            y_4, 
            **params
        )
        updated_model.save('/opt/airflow/models/result_model.h5')
        
        with open(path, 'w') as f:
            json.dump(str(last_update), f)
    else:
        print('too little matches to update')    