import requests
import json
import pandas as pd
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

# function sending data to PostgreSQL db
def data_to_sql(table_name, df, pg_hook, conflict_columns, update_columns = None):
    conn = None
    cur = None
    try:
        # Establish the connection
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        #insert data into tables
        if len(conflict_columns) == 0:
            insert_query = """
                INSERT INTO {} ({})
                VALUES ({})
            """.format(table_name, ','.join(df.columns), ','.join(['%s']*len(df.columns)))
        else:
            if update_columns:
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                insert_query = """
                    INSERT INTO {} ({})
                    VALUES ({})
                    ON CONFLICT ({}) DO UPDATE SET {}
                """.format(table_name, ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns), update_set)

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

# function inserting data into  seasons df
def insert_seasons(response):
    season_rows = []
    for league in response['response']:
        for season in league['seasons']:
            row = {}
            row['league_id'] = league['league']['id']
            for key, val in season.items():

                if key == 'coverage':
                    for k, v in season['coverage'].items():
                        if k == 'fixtures':
                            for a, b in season['coverage']['fixtures'].items():
                                row[a] = b
                        else:
                            row[k] = v
                else:
                    row[key] = val
            season_rows.append(row)
    
    seasons = pd.DataFrame(season_rows)
    return seasons

def insert_contests(response):
    contests_rows = []
    for league in response['response']:
        contest_row = {'league_id': league['league']['id'],
                       'name': league['league']['name'],
                       'type': league['league']['type'],
                       'logo': league['league']['logo'],
                       'country': league['country']['name'],
                       'country_code': league['country']['code'],
                       'seasons': len(league['seasons'])}
        contests_rows.append(contest_row)
    contests = pd.DataFrame(contests_rows)
    return contests

def check():

    with open('/opt/airflow/data/new_seasons.json', 'r') as file:
        new_seasons = json.load(file)

    remaining = 1

    if len(new_seasons) > 0:
        remaining = get_new_seasons(new_seasons, remaining)

    if remaining > 0:

        leagues, remaining = get_data('leagues', {})

        if len(leagues['response']) > 0:
            contests = insert_contests(leagues)
            seasons = insert_seasons(leagues)

            uefa = pd.read_csv('/opt/airflow/data/UEFA_Ranking_2024.csv')
            eur_cs = list(uefa['country'].unique())
            contests = contests[contests['country'].isin(eur_cs)]
            eur_ss = list(contests['league_id'].unique())
            eur_seasons = seasons[seasons['league_id'].isin(eur_ss)]
            eur_seasons = eur_seasons.merge(contests[['league_id','country','name']], on='league_id')
            eur_seasons = eur_seasons[[
                'league_id', 'name', 'country', 
                'year', 'players', 'statistics_fixtures', 
                'current', 'predictions', 'odds'
                ]]
            eur_seasons_old = pd.read_csv('/opt/airflow/data/european_seasons.csv')
            diff_df = eur_seasons.merge(eur_seasons_old[['league_id','year']], on=['league_id','year'], how='left', indicator=True)
            only_in_df2 = diff_df[diff_df['_merge'] == 'left_only'].drop(columns='_merge')
            eur_seasons.to_csv('/opt/airflow/data/european_seasons.csv')
            new_seasons = list(zip(only_in_df2['league_id'],only_in_df2['year']))
            with open('/opt/airflow/data/new_seasons.json', 'w') as file:
                json.dump(new_seasons, file)
            print(f'new {len(new_seasons)} seasons found')
            
            if len(new_seasons) > 0:
                remaining = get_new_seasons(new_seasons, remaining)

            return new_seasons, int(remaining)
    else:
        print('no requests left, couldnt get seasons')

def get_new_seasons(new_seasons, remaining):
    fixtures_data = []
    endpoint = 'fixtures'
    while len(new_seasons) > 0 and remaining > 0:
        params = {
            'league': new_seasons[0][0],
            'season': new_seasons[0][1]
            }
        fixtures, remaining_req = get_data(endpoint, params)
        if len(fixtures['response']) > 0:
            fixtures_data.extend(encode_data(fixture) for fixture in fixtures['response'])
        remaining = int(remaining_req)
        new_seasons.pop(0)

    with open('/opt/airflow/data/new_seasons.json', 'w') as file:
        json.dump(new_seasons, file)
    
    if len(fixtures_data) > 0:
        fixtures_df = pd.DataFrame(fixtures_data)
        fixtures_df = fixtures_df.drop(columns = {
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
        fixtures_df = fixtures_df.fillna(0)
        conflict_col = ['fixture_id']
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        data_to_sql('fixtures', fixtures_df, pg_hook, conflict_col)
        print('fixtures data send')
    return remaining
    