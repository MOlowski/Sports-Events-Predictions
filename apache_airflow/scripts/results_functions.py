from datetime import date
import pandas as pd
import json
import requests
from airflow.hooks.postgres_hook import PostgresHook
from dotenv import load_dotenv  
import os

def get_data(endpoint, params):

    load_dotenv()  # Load environment variables from .env file

    api_key = os.getenv('API_KEY')

    if api_key is None:
        raise ValueError("API key not set.")

    URL = "https://v3.football.api-sports.io/"
    headers = {
	'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': api_key
    }    
    
    response = requests.get(
        URL+endpoint,
        headers = headers,
        params = params
    )
    if response.status_code == 200:
            
        remaining = response.headers.get("x-ratelimit-requests-remaining")
        data = response.json()
        print(f"requests before reaching limit {remaining}")

    else:
        print(f"Error {response.status_code}, {response.text}")

    return data, remaining     

def encode_data(data_dict, parent_key = '', sep= '_'):
    encoded = []
    for key, val in data_dict.items():
        new_key = f'{parent_key}{sep}{key}' if parent_key else key
        if isinstance(val, dict):
            encoded.extend(encode_data(val, new_key, sep=sep).items())
        elif isinstance(val, list):
            if val:
                if all(isinstance(i, dict) for i in val):
                    for k, v in enumerate(val):
                        v_key = f'{new_key}{sep}{k}'
                        encoded.extend(encode_data(v, v_key, sep=sep).items())
                else:
                    encoded.append((new_key, val))
            else:
                encoded.append((new_key, []))
        else:
            encoded.append((new_key, val))
    return dict(encoded)                                                                

def data_to_sql(table_name, df, conflict_columns, update_columns = None):
    conn = None
    cur = None
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    try:
        # Establish the connection
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        if not update_columns:
            update_columns = [col for col in df.columns if col not in conflict_columns]
        #insert data into tables
        if len(conflict_columns) == 0:
            insert_query = """
                INSERT INTO {} ({})
                VALUES ({})
            """.format(table_name, ','.join(df.columns), ','.join(['%s']*len(df.columns)))
        else:
            if update_columns:
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                insert_query = f"""
                    INSERT INTO {table_name} ({','.join(df.columns)})
                    VALUES ({','.join(['%s'] * len(df.columns))})
                    ON CONFLICT ({','.join(conflict_columns)}) DO UPDATE SET {update_set}
                """
            else:
                insert_query = """
                    INSERT INTO {} ({})
                    VALUES ({})
                    ON CONFLICT ({}) DO NOTHING
                """.format(table_name, ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns))
        
        cur.executemany(insert_query, df.values.tolist())
        print(f'table {table_name} updated')
        
        # Commit the changes
        conn.commit()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            # Close the cursor and connection
            cur.close()
        if cur is not None:
            conn.close()

def get_results():

    today = date.today()
    es = pd.read_csv('/opt/airflow/data/european_seasons.csv')
    s = pd.read_csv('/opt/airflow/data/seasons.csv')
    seasons = pd.merge(es,s[['league_id','year','start','end']], on = ['league_id', 'year'])
    seasons['start'] = pd.to_datetime(seasons['start']).dt.date
    seasons['end'] = pd.to_datetime(seasons['end']).dt.date

    current_seasons = seasons[(seasons['start']<today)&(seasons['end']>today)][['league_id', 'year']]
    to_get = current_seasons.values.tolist()

    remaining = 100
    done = False
    fix_data = []

    path = '/opt/airflow/data/last.json'
    with open(path, 'r') as file:
        last = json.load(file)

    if len(last)>0:
        if not last[2]:
            ind = to_get.index([last[0], last[1]])
            to_get = to_get[ind:]

    if today.weekday() < 1 & len(last) == 0:
        done = True

    while (remaining > 0)&(done == False):
        params = {'league' : to_get[0][0], 'season': to_get[0][1]}
        data, remaining = get_data('fixtures', params)
        remaining = int(remaining)
        if len(data['response'])>0:
            fix_data.extend(encode_data(fix) for fix in data['response'])
        
        to_get.pop(0)
        if len(to_get) == 0:
            done = True

    if remaining <= 0 and not done:
        last = [to_get[0][0],to_get[0][1], done]
        path = 'data/last.json'
        with open(path, 'w') as file:
            json.dump(last, file)
    else:
        with open(path, 'w') as file:
            json.dump([], file)

    df = pd.DataFrame(fix_data)
    print(len(df))
    if len(fix_data) > 0:
        df = df.drop(columns = {
            'fixture_timezone',
            'fixture_timestamp', 
            'fixture_periods_first', 
            'fixture_periods_second',
            'league_name',
            'league_country',
            'league_logo',
            'league_flag',
            'fixture_venue_city',
            'fixture_venue_name',
            'teams_away_logo',
            'teams_away_name',
            'teams_home_logo',
            'teams_home_name'})
        update_cols = list(df.columns)
        update_cols.pop(0)
        df = df.fillna(0)
        data_to_sql('fixtures', df, ['fixture_id'], update_cols)

def get_bet_res(bets):
 
    if bets['bet_name'] == 'result_home':
        bets['match_result'] = True if bets['goals_home'] > bets['goals_away'] else False
    elif bets['bet_name'] == 'result_draw':
        bets['match_result'] = True if bets['goals_home'] == bets['goals_away'] else False
    elif bets['bet_name'] == 'result_away':
        bets['match_result'] = True if bets['goals_home'] < bets['goals_away'] else False
    elif bets['bet_name'] == 'both_scores_true':
        bets['match_result'] = True if bets['goals_home'] > 0 and bets['goals_away'] > 0 else False
    elif bets['bet_name'] == 'both_scores_false':
        bets['match_result'] = True if bets['goals_home'] < 1 or bets['goals_away'] < 1 else False
    elif bets['bet_name'] == 'double_chance_home':
        bets['match_result'] = True if bets['goals_home'] >= bets['goals_away'] else False
    elif bets['bet_name'] == 'double_chance_away':
        bets['match_result'] = True if bets['goals_home'] <= bets['goals_away'] else False
    elif bets['bet_name'] == 'fh_result_home':
        bets['match_result'] = True if bets['score_halftime_home'] > bets['score_halftime_away'] else False
    elif bets['bet_name'] == 'fh_result_draw':
        bets['match_result'] = True if bets['score_halftime_home'] == bets['score_halftime_away'] else False
    elif bets['bet_name'] == 'fh_result_away':
        bets['match_result'] = True if bets['score_halftime_home'] < bets['score_halftime_away'] else False
    elif bets['bet_name'] == 'home_over_1':
        bets['match_result'] = True if bets['goals_home'] > 1 else False
    elif bets['bet_name'] == 'home_over_2':
        bets['match_result'] = True if bets['goals_home'] > 2 else False
    elif bets['bet_name'] == 'away_over_1':
        bets['match_result'] = True if bets['goals_away'] > 1 else False
    else:
        bets['match_result'] = True if bets['goals_away'] > 2 else False

    return bets

def check_bets():
    conn = None
    cur = None
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    try:
        # Establish the connection
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        query = ''' 
    SELECT b.*, f.goals_home, f.goals_away, f.score_halftime_home, f.score_halftime_away
    FROM bets b
    LEFT JOIN fixtures f
    ON b.fixture_id = f.fixture_id
    WHERE f.fixture_status_short in ('FT', 'WO', 'AET', 'PEN')
    '''
        df = pd.read_sql_query(query, conn)
        df = df.apply(get_bet_res, axis=1)
        df = df[['fixture_id', 'bet_name', 'bet_number', 'match_result']]
        df['bet_result'] = df.groupby('bet_number')['match_result'].transform(lambda x: True if (x==True).all() else False)
        conflict_columns = ['fixture_id', 'bet_name']

        insert_query = """
            INSERT INTO {} ({})
            VALUES ({})
            ON CONFLICT ({}) DO NOTHING
        """.format('bets_results', ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns))

        cur.executemany(insert_query, df.values.tolist())
        
        # Commit the changes
        conn.commit()
        
    except Exception as e:
        print(f"Error: {e}")
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            # Close the cursor and connection
            cur.close()
        if cur is not None:
            conn.close()

