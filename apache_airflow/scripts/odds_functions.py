import requests
import re
import pandas as pd
from datetime import date
import datetime
from airflow.hooks.postgres_hook import PostgresHook
import psycopg2
import time
import json
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

def get_predicted_matches():

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    #get next friday and monday dates as start and end for query
    t = date.today()
    start_date = t
    while start_date.weekday() != 4:
        if start_date.weekday() < 4:
            start_date += datetime.timedelta(1)
        else:
            start_date -= datetime.timedelta(1)
    end_date = start_date
    while end_date.weekday() != 0:
        end_date += datetime.timedelta(1)

    conn = None
 
    # get upocoming matches playing from next friday to monday
    try:
        conn = pg_hook.get_conn()

        query = '''
    SELECT a.*
    FROM (
        SELECT p.*, f.league_id, fixture_date
        FROM predictions p
        JOIN fixtures f ON p.fixture_id = f.fixture_id
        WHERE f.fixture_date >= '{}' and fixture_date <= '{}'
    ) a
    LEFT JOIN odds o ON a.fixture_id = o.fixture_id
    WHERE o.fixture_id IS NULL;
    '''.format(start_date, end_date)
        current_matches = pd.read_sql_query(query, conn)
        
        print(start_date, end_date, len(current_matches))
        return current_matches, start_date, end_date
    except Exception as e:
        print(f'Error {e}')
        return None, None, None
    finally:
        if conn is not None:
            conn.close()

def send_to_sql(df):
    if len(df) > 0:
        df = preprocess_data(df)

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
        """.format('odds', ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns))

        cur.executemany(insert_query, df.values.tolist())
        
        # Commit the changes
        conn.commit()
        return print(f'table odds updated')
    except Exception as e:
        print(f'Error {e}')
    
    finally:
        if conn is not None:
            # Close the cursor and connection
            cur.close()
        if cur is not None:
            conn.close()

def preprocess_data(odd_data):
    
    all_filtered_data = []
    for index, data in enumerate(odd_data):
        books = [len(book['bets']) for book in odd_data[index]['bookmakers']]
        bb = books.index(max(books))
        data = encode_data(data)
        filtered_data = {}
        filtered_data['fixture_id'] = data['fixture_id']
        
        for key, value in data.items():
            if value == 'Match Winner':
                match = re.search(fr'bookmakers_{bb}_bets_(\d+)_name', key)
                if match:
                    bet_number = match.group(1)
                    if data[f'bookmakers_{bb}_bets_{bet_number}_values_0_value'] == 'Home':
                        filtered_data['result_home'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    elif data[f'bookmakers_{bb}_bets_{bet_number}_values_0_value'] == 'Draw':
                        filtered_data['result_draw'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    if (f'bookmakers_{bb}_bets_{bet_number}_values_1_odd' in data) & (data[f'bookmakers_{bb}_bets_{bet_number}_values_1_value'] == 'Away'):
                            filtered_data['result_away'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_1_odd']
                    else:
                        filtered_data['result_draw'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_1_odd']
                    if f'bookmakers_{bb}_bets_{bet_number}_values_2_odd' in data:
                        filtered_data['result_away'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_2_odd']
            if value == 'Both Teams Score':
                match = re.search(fr'bookmakers_{bb}_bets_(\d+)_name', key)
                if match:
                    bet_number = match.group(1)
                    filtered_data['both_scores_true'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    filtered_data['both_scores_false'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_1_odd']
            if value == 'Double Chance':
                match = re.search(fr'bookmakers_{bb}_bets_(\d+)_name', key)
                if match:
                    bet_number = match.group(1)
                    if data[f'bookmakers_{bb}_bets_{bet_number}_values_0_value'] == 'Home/Draw':
                        filtered_data['double_chance_home'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    elif data[f'bookmakers_{bb}_bets_{bet_number}_values_0_value'] == 'Draw/Away':
                        filtered_data['double_chance_away'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    if (f'bookmakers_{bb}_bets_{bet_number}_values_1_odd' in data) & (data[f'bookmakers_{bb}_bets_{bet_number}_values_1_value'] == 'Draw/Away'):
                            filtered_data['double_chance_away'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_1_odd']
                    if f'bookmakers_{bb}_bets_{bet_number}_values_2_odd' in data:
                        filtered_data['double_chance_away'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_2_odd']
            if value == 'First Half Winner':
                match = re.search(fr'bookmakers_{bb}_bets_(\d+)_name', key)
                if match:
                    bet_number = match.group(1)
                    if data[f'bookmakers_{bb}_bets_{bet_number}_values_0_value'] == 'Home':
                        filtered_data['fh_result_home'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    elif data[f'bookmakers_{bb}_bets_{bet_number}_values_0_value'] == 'Draw':
                        filtered_data['fh_result_draw'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    if (f'bookmakers_{bb}_bets_{bet_number}_values_1_odd' in data) & (data[f'bookmakers_{bb}_bets_{bet_number}_values_1_value'] == 'Away'):
                            filtered_data['fh_result_away'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_1_odd']
                    else:
                        filtered_data['fh_result_draw'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_1_odd']
                    if f'bookmakers_{bb}_bets_{bet_number}_values_2_odd' in data:
                        filtered_data['fh_result_away'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_2_odd']
            if value == 'Total - Home':
                match = re.search(fr'bookmakers_{bb}_bets_(\d+)_name', key)
                if match:
                    bet_number = match.group(1)
                    filtered_data['home_over_1'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    filtered_data['home_over_2'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_2_odd']
            if value == 'Total - Away':
                match = re.search(fr'bookmakers_{bb}_bets_(\d+)_name', key)
                if match:
                    bet_number = match.group(1)
                    filtered_data['away_over_1'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_0_odd']
                    filtered_data['away_over_2'] = data[f'bookmakers_{bb}_bets_{bet_number}_values_2_odd']
                    
            cols = ['result_home', 'result_draw', 
                    'result_away', 'both_scores_true', 
                    'both_scores_false', 'double_chance_home', 
                    'double_chance_away', 'fh_result_home', 
                    'fh_result_draw', 'fh_result_away', 
                    'home_over_1', 'home_over_2', 
                    'away_over_1', 'away_over_2']
        for col in cols:
            if col not in filtered_data:
                filtered_data[col] = 0
        all_filtered_data.append(filtered_data)
    df = pd.DataFrame(all_filtered_data)
    return df

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

def get_odds(matches, start_date, end_date):

    #get only matches with odds available
    eur_seasons = pd.read_csv('/opt/airflow/data/european_seasons.csv')
    matches = matches.merge(eur_seasons[['league_id','odds']], on='league_id')
    matches = matches[matches['odds']]

    with open ('/opt/airflow/data/odds.json', 'r') as f:
        left = json.load(f)
    if len(left[1]) > 0:
        if left[0].isoformat() == start_date:
            leagues_list = left

    leagues_list = list(matches['league_id'].unique())
    print(f"{len(leagues_list)*4} requests needed")
    odds_data = []
    remaining = 10000
    done = False
    date = start_date
    page = 1

    while remaining > 0 and not done:

        season = int(eur_seasons[eur_seasons['league_id']==leagues_list[0]]['year'].max())
        print(leagues_list[0], season, date)
        params = {'league':leagues_list[0],
                'date':date,
                'season':season,
                'page':page}
        response, remaining = get_data('odds', params) 

        if page != response['paging']['total']:
            page += 1
        else:
            page = 1
            if date != end_date:
                date += datetime.timedelta(1)
            else:
                date = start_date
                leagues_list.pop(0)
        time.sleep(6)
        if len(response['response']) > 0:
            odds_data.extend(match for match in response['response'])

        remaining = int(remaining)
        done = True if len(leagues_list)==0 else False

    if done:
        print('whole odds were collected for upcoming matches')
    else:
        print('not every odd were collected for upcoming matches')
    
    with open ('/opt/airflow/data/odds.json', 'w') as f:
        json.dump((start_date.isoformat(), leagues_list), f)

    return odds_data
