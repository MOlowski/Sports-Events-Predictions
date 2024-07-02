import psycopg2
import numpy as np
import pandas as pd
import joblib
import datetime
from datetime import date
from airflow.hooks.postgres_hook import PostgresHook
from tensorflow.keras.models import load_model

# function finding upcoming matches
def get_current_matches():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = None
    #get next friday and monday dates as start and end for query
    t = date.today()
    print('today', t)
    start_date = t+datetime.timedelta(1) if t.weekday() == 4 else t
    while start_date.weekday() != 4:
        start_date = start_date+datetime.timedelta(1)
    end_date = start_date
    while end_date.weekday() != 0:
        end_date = end_date+datetime.timedelta(1)
    print('matches from', start_date, 'to', end_date)
    # get upocoming matches playing from next friday to monday
    try:
        conn = pg_hook.get_conn()

        query = '''
    SELECT * 
    FROM fixtures
    WHERE fixture_date >= '{}' and fixture_date <= '{}' and fixture_status_short = 'NS'
    '''.format(start_date, end_date)
        current_matches = pd.read_sql_query(query, conn)
        print('got current matches')
        return current_matches
    except Exception as e:
        print(f'Error {e}')
        return None, None
    finally:
        if conn is not None:
            conn.close()

# get last matches for each league in current matches
def get_last_matches(leagues, seasons):

    conn = None
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    #get next friday and monday dates as start and end for query
    t = date.today()
    end_date = t+datetime.timedelta(1) if t.weekday() == 0 else t
    while end_date.weekday() != 0:
        end_date = end_date+datetime.timedelta(1)
        
    # get upocoming matches playing from next friday to monday
    try:
        conn = pg_hook.get_conn()
        last_matches_date = end_date-datetime.timedelta(14)
        query2 = '''
    SELECT *
    FROM fixtures
    WHERE fixture_status_short IN ('FT', 'WO', 'AET', 'PEN', 'CANC') 
    AND league_id = ANY(%s) 
    AND league_season = ANY(%s) 
    '''
        last_matches = pd.read_sql_query(query2, conn, params=(leagues, seasons))
        print('got last matches')
        return last_matches, last_matches_date
    except Exception as e:
        print(f'Error {e}')
        return None, None
    finally:
        if conn is not None:
            conn.close()

# add features which are entries for predicting model to last matches
def add_statistics(fixtures_df):
        
    fixtures_df['fixture_date'] = pd.to_datetime(fixtures_df['fixture_date']).dt.date
    fixtures_df = fixtures_df.sort_values(by='fixture_date')
    fixtures_df['teams_home_goals_scored_home'] = fixtures_df.groupby(['league_season', 'teams_home_id'])['goals_home'].cumsum()
    fixtures_df['teams_away_goals_scored_away'] = fixtures_df.groupby(['league_season','teams_away_id'])['goals_away'].cumsum()
    fixtures_df['teams_home_goals_lost_home'] = fixtures_df.groupby(['league_season','teams_home_id'])['goals_away'].cumsum()
    fixtures_df['teams_away_goals_lost_away'] = fixtures_df.groupby(['league_season','teams_away_id'])['goals_home'].cumsum()
    fixtures_df['teams_home_winner'] = fixtures_df.apply(lambda row: 3 if row['goals_home']>row['goals_away'] else (1 if row['goals_home']==row['goals_away']  else 0), axis=1)
    fixtures_df['teams_away_winner'] = fixtures_df.apply(lambda row: 0 if row['goals_home']>row['goals_away'] else (1 if row['goals_home']==row['goals_away']  else 3), axis=1)
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
    total['total_goals_scored'] = total[['fixture_date','league_season','team_id','goals_scored']].groupby(['league_season','team_id'])['goals_scored'].cumsum()
    total['total_goals_lost'] = total[['fixture_date','league_season','team_id','goals_lost']].groupby(['league_season','team_id'])['goals_lost'].cumsum()
    
    total = total.sort_values(by='fixture_date')
    total['total_points'] = total[['fixture_date', 'league_season', 'team_id', 'league_round', 'points']].groupby(['league_season','team_id'])['points'].cumsum()

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

# function predicting matches results
def get_preprocess_data():

    current = get_current_matches()
    if len(current) == 0:
        print('no upcoming matches this weekend')
    else:
        leagues = list(current['league_id'].unique())
        seasons = list(current['league_season'].unique())
        leagues = [int(league) for league in leagues]
        seasons = [str(season) for season in seasons]
        last, end_date = get_last_matches(leagues, seasons)

        last = add_statistics(last)
        last = last[last['fixture_date'] > end_date]
        # add last match result and goals to columns


        # get only matches where teams played in last 14 days
        teams = list(last['teams_home_id'].unique())
        teams = teams + list(last['teams_away_id'].unique())
        teams = list(dict.fromkeys(teams))
        
        current = current[(current['teams_home_id'].isin(teams))&(current['teams_away_id'].isin(teams))]
        
        # chose only needed columns
        predict_df = current[['fixture_id', 'fixture_date', 'fixture_venue_id', 'league_id', 'league_season', 'teams_home_id', 'teams_away_id']]
        
        # get values needed for prediction from last matches (points, goals, standings, etc.)
        def team_values(team, column, is_sensitive, last):

            # last match for team
            matches = last[(last['teams_home_id']==team)|(last['teams_away_id']==team)]
            if matches.empty:
                # Handle case where no matches are found for the team
                return None
            last_date_index = matches['fixture_date'].idxmax()
            last_match = matches.loc[last_date_index]

        # return needed column for given team
            if last_match['teams_home_id']==team:
                if is_sensitive:
                    return last_match[f'teams_home_{column}_home']
                else:
                    return last_match[f'teams_home_{column}']
            else:
                if is_sensitive:
                    return last_match[f'teams_away_{column}_away']
                else:
                    return last_match[f'teams_away_{column}']
        cols = [
            'total_goals_scored', 
            'total_goals_lost',
            'total_points', 
            'standings',
            'last_five_matches_points']

        for row in cols:
            predict_df[f'teams_home_{row}'] = predict_df['teams_home_id'].apply(lambda x: team_values(x, row, False, last))
            predict_df[f'teams_away_{row}'] = predict_df['teams_away_id'].apply(lambda x: team_values(x, row, False, last))

        sensitive_cols = ['goals_scored', 'goals_lost']

        for row in sensitive_cols:
            predict_df[f'teams_home_{row}_home'] = predict_df['teams_home_id'].apply(lambda x: team_values(x, row, True, last))
            predict_df[f'teams_away_{row}_away'] = predict_df['teams_away_id'].apply(lambda x: team_values(x, row, True, last))

        print(predict_df.head())
        return predict_df

# function predicting matches
def predict(predict_df):
    print(predict_df.head())
    # load season df to add league type to predict df
    s_path = '/opt/airflow/data/contests.csv'
    seasons = pd.read_csv(s_path)
    predict_df['fixture_date'] = pd.to_datetime(predict_df['fixture_date'])
    predict_df['day_of_week'] = predict_df['fixture_date'].dt.dayofweek
    predict_df = predict_df.merge(seasons[['league_id','type']], on='league_id', how='left')

    # load encoder and encode league type column
    label_encoder = joblib.load('/opt/airflow/models/label_encoder_league_type.pkl')
    predict_df['league_type_encoded'] = label_encoder.transform(predict_df['type'])

    # get proper dfs for predictions
    X_goals = predict_df[[
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
    X_result = predict_df[[
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

    # goals prediction
    goals_model = load_model('/opt/airflow/models/goal_model.h5')

    goal_prediction = goals_model.predict(X_goals)

    # get binary representation
    goal_binary_prediction = [pred > 0.5 for pred in goal_prediction]

    # make df from predicted values
    flatten_g_preds = np.hstack(goal_prediction)
    flatten_g_b_preds = np.hstack(goal_binary_prediction)
    columns_g = [
        'home_over_1_prob', 'home_over_2_prob', 
        'away_over_1_prob', 'away_over_2_prob', 
        'both_scores_prob'
    ]
    binary_columns_g = [
        'home_over_1_pred', 'home_over_2_pred', 
        'away_over_1_pred', 'away_over_2_pred', 
        'both_scores_pred'
    ]

    pred_g_df = pd.DataFrame(flatten_g_preds, columns=columns_g)
    pred_g_b_df = pd.DataFrame(flatten_g_b_preds, columns=binary_columns_g)
    predicted_g_df = pd.concat([predict_df[['fixture_id','teams_home_id','teams_away_id']].reset_index(drop=True), pred_g_df, pred_g_b_df], axis=1)
    
    # result prediction
    result_model = load_model('/opt/airflow/models/result_model.h5')

    result_prediction = result_model.predict(X_result)

    # get categorical and binary represenatation 
    result_pred = np.argmax(result_prediction[0], axis=1)
    result_first_half_pred = np.argmax(result_prediction[1], axis=1)
    result_double_chance_home_pred = (result_prediction[2]>0.5)
    result_double_chance_away_pred = (result_prediction[3]>0.5)

    result_prob = np.max(result_prediction[0], axis=1)
    result_first_half_prob = np.max(result_prediction[1], axis=1)

    # make df from predicted values
    predicted_r_df = pd.DataFrame({
        'fixture_id': predict_df['fixture_id'],
        'result_pred': result_pred.flatten(),
        'result_first_half_pred': result_first_half_pred.flatten(),
        'result_double_chance_home_pred': result_double_chance_home_pred.flatten(),
        'result_double_chance_away_pred': result_double_chance_away_pred.flatten(),
        'result_prob': result_prob.flatten(),
        'result_first_half_prob': result_first_half_prob.flatten(),
        'result_double_chance_home_prob': result_prediction[2].flatten(),
        'result_double_chance_away_prob': result_prediction[3].flatten()
    })

    # merge goals and results predictions to one df
    predictions = pd.merge(predicted_g_df, predicted_r_df, on='fixture_id')

    return predictions

# send data to db
def send_to_sql(df):
    conn = None
    cur = None
    conflict_columns = ['fixture_id']
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    try:
    
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        
        insert_query = """
            INSERT INTO {} ({})
            VALUES ({})
            ON CONFLICT ({}) DO NOTHING
        """.format('predictions', ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns))

        cur.executemany(insert_query, df.values.tolist())
        
        # Commit the changes
        conn.commit()
        return print(f'table predictions updated')
    except Exception as e:
        print(f'Error {e}')
    
    finally:
        if conn is not None:
            # Close the cursor and connection
            cur.close()
        if cur is not None:
            conn.close()

