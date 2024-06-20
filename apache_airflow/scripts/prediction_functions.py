import psycopg2
import numpy as np
import pandas as pd
import datetime
from datetime import date
from airflow.hooks.postgres_hook import PostgresHook
from sklearn.preprocessing import LabelEncoder
import joblib
from tensorflow.keras.models import load_model

# function finding upcoming matches and last matches(needed for model entries)
def get_current_last_matches():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = None
    #get next friday and monday dates as start and end for query
    t = date.today()
    start_date = t+datetime.timedelta(1) if t.weekday() == 4 else t
    end_date = t+datetime.timedelta(1) if t.weekday() == 0 else t
    while start_date.weekday() != 4:
        start_date += datetime.timedelta(1)
    while end_date.weekday() != 0:
        end_date += datetime.timedelta(1)
        
    # get upocoming matches playing from next friday to monday
    try:
        conn = psycopg2.connect(pg_hook)

        query = '''
    SELECT * 
    FROM fixtures
    WHERE fixture_date >= '{}' and fixture_date <= '{}' and fixture_status_short = 'NS'
    '''.format(start_date, end_date)
        current_matches = pd.read_sql_query(query, conn)

        last_matches_date = end_date-datetime.timedelta(14)
        query2 = '''
    SELECT *
    FROM fixtures_updated
    WHERE fixture_date >= '{}' and fixture_status_short IN ('FT', 'WO', 'AET', 'PEN', 'CANC')
    '''.format(last_matches_date)
        last_matches = pd.read_sql_query(query2, conn)
        
        return current_matches, last_matches
    except Exception as e:
        print(f'Error {e}')
        return None, None
    finally:
        if conn is not None:
            conn.close()

# function predicting matches results
def get_preprocess_data():

    current, last = get_current_last_matches()

    # get only matches where teams played in last 14 days
    teams = list(last['teams_home_id'].unique())
    teams = teams + list(last['teams_away_id'].unique())
    teams = list(dict.fromkeys(teams))
    current = current[(current['teams_home_id'].isin(teams))&(current['teams_away_id'].isin(teams))]
    
    # chose only needed columns
    predict_df = current[['fixture_id', 'fixture_date', 'fixture_venue_id', 'league_id', 'league_season', 'teams_home_id', 'teams_away_id']]
    
    # get values needed for prediction from last matches (points, goals, standings, etc.)
    def team_values(team, column, is_sensitive):

        # last match for team
        matches = last[(last['teams_home_id']==team)|(last['teams_away_id']==team)]
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

    # create column with values needed for prediction
    for row in cols:
        predict_df[f'teams_home_{row}'] = predict_df.apply(lambda x: team_values(x['teams_home_id'], row, False), axis=1)
    for row in cols:
        predict_df[f'teams_away_{row}'] = predict_df.apply(lambda x: team_values(x['teams_away_id'], row, False), axis=1)

    sensitive_cols = ['goals_scored', 'goals_lost']

    for row in sensitive_cols:
        predict_df[f'teams_home_{row}_home'] = predict_df.apply(lambda x: team_values(x['teams_home_id'], row, True), axis=1)
    for row in sensitive_cols:
        predict_df[f'teams_away_{row}_away'] = predict_df.apply(lambda x: team_values(x['teams_away_id'], row, True), axis=1)

    return predict_df

# function predicting matches
def predict(predict_df):

    # load season df to add league type to predict df
    s_path = '/opt/airflow/data/contests.csv'
    seasons = pd.read_csv(s_path)
    predict_df['fixture_date'] = pd.to_datetime(predict_df['fixture_date'])
    predict_df['day_of_week'] = predict_df['fixture_date'].dt.dayofweek
    predict_df = predict_df.merge(seasons[['league_id','type']], on='league_id', how='left')

    # load encoder and encode league type column
    label_encoder = joblib.load('models/label_encoder_league_type.pkl')
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
    goals_model = load_model('models/goal_model.h5')

    goal_prediction = goals_model.predict(X_goals)

    # get binary representation
    goal_binary_prediction = [pred > 0.5 for pred in goal_prediction]

    # make df from predicted values
    flatten_g_preds = np.hstack(goal_prediction)
    flatten_g_b_preds = np.hstack(goal_binary_prediction)
    columns_g = [
        'home_over_1_prob', 'home_over_2_prob', 'home_over_3_prob', 
        'away_over_1_prob', 'away_over_2_prob', 'away_over_3_prob', 
        'both_scores_prob'
    ]
    binary_columns_g = [
        'home_over_1_pred', 'home_over_2_pred', 'home_over_3_pred', 
        'away_over_1_pred', 'away_over_2_pred', 'away_over_3_pred', 
        'both_scores_pred'
    ]

    pred_g_df = pd.DataFrame(flatten_g_preds, columns=columns_g)
    pred_g_b_df = pd.DataFrame(flatten_g_b_preds, columns=binary_columns_g)
    predicted_g_df = pd.concat([predict_df[['fixture_id','teams_home_id','teams_away_id']].reset_index(drop=True), pred_g_df, pred_g_b_df], axis=1)
    
    # result prediction
    result_model = load_model('models/result_model.h5')

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
    
        conn = psycopg2.connect(pg_hook)
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

