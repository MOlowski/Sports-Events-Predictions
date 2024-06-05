import requests
import psycopg2
import numpy as np
import pandas as pd
import json
import os
import time

# function requesting data from Football API
def get_data(endpoint, params):
    
    URL = "https://v3.football.api-sports.io/"
    headers = {
	'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': "fb2140228973d644db847895c454c22b"
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

# preprocessing data, basicly converting json data to pandas dataframe                                                     
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

# preprocessing data, same as above
def encode_fix_stats(data, fixture_id):
    encoded = []
    encoded.append(('fixture_id', fixture_id))
    for key, val in data.items():
        if key =='team':
            encoded.append((key+'_id', val['id']))
        else:
            for el in val:
                encoded.append((el['type'].lower().replace(' ', '_').replace('%', 'percentage'), el['value']))
    return dict(encoded)

# function sending data to PostgreSQL db
def data_to_sql(table_name, df, db_params, conflict_columns):
    conn = None
    cur = None
    try:
        # Establish the connection
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        #insert data into tables
        if len(conflict_columns) == 0:
            insert_query = """
                INSERT INTO {} ({})
                VALUES ({})
            """.format(table_name, ','.join(df.columns), ','.join(['%s']*len(df.columns)))
        else:
            insert_query = """
                INSERT INTO {} ({})
                VALUES ({})
                ON CONFLICT ({}) DO NOTHING
            """.format(table_name, ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns))
        if len(df) > 0:
            last_row = df.iloc[-1]
        cur.executemany(insert_query, df.values.tolist())
        print(f'table {table_name} updated')
        
        # Commit the changes
        conn.commit()
        
    except Exception as e:
        print(f"Error: {e}")
        if last_row is not None:
            print(f"Last row loaded before the error occurred: {last_row}")
    finally:
        if conn is not None:
            # Close the cursor and connection
            cur.close()
        if cur is not None:
            conn.close()

# get list of matches to collect matches stats
def get_matches(db_params):
    query = '''
    SELECT fixture_id
    FROM fixtures
    WHERE fixture_status_short = 'FT' or fixture_status_short = 'WO' or fixture_status_short = 'AET' or fixture_status_short = 'PEN' or fixture_status_short = 'CANC'
    '''
    res = []
    conn = None
    cur = None
    
    try:
        conn = psycopg2.connect(db_params)
        cur = conn.cursor()

        cur.execute(query)
        res = [row[0] for row in cur.fetchall()]
    
    except Exception as e:
        print(f'Error {e} occured')

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
            
        return res
    
# get past data about teams and matches
def get_and_preproc_historical_data():
    current_path = '/opt/airflow/data/current.csv'
    current = pd.read_csv(current_path)
    european_s_path = '/opt/airflow/data/european_seasons.csv'
    european_seasons = pd.read_csv(european_s_path)

    # insert parameters to be found
    league = current['league_id'][0]
    year = current['year'][0]
    to_find = current['type'][0]

    # get current index
    index = np.where((european_seasons['league_id']==league)&(european_seasons['year']==year))[0][0]

    remaining = 100
    total_team_stats_data = []
    total_teams_data = []
    total_fixtures_data = []

    if index == len(european_seasons)-1:
        done = True
    else:
        done = False

    # if script ended on collecting team stats get rest of teams
    if to_find == 'teams/statistics':
        teams_path = '/opt/airflow/data/teams.json'
        with open(teams_path, 'r') as file:
            teams_list = json.load(file)

    else:
        teams_list = []
    # if limit of requests wasnt reached and there is still something to collect enter loop
    while (done==False)&(remaining > 0):

        # get data from API
        if to_find == 'teams': 
            params = {
                'league': league,
                'season': year
                    }
            print('teams')
        elif to_find == 'teams/statistics':
            team = teams_list[0]
            params = {
                'league': league,
                'season': year,
                'team': team
                    }
            print(f'team {team} stats from {year}, {len(teams_list)} teams left')
        else:
            params = {
                'league': league,
                'season': year
                }
            print('fixtures')
        
        endpoint = to_find
        data, remaining_req = get_data(endpoint, params)
        
        # find all teams played in each league in each season
        if endpoint == 'teams':
            total_teams_data.extend(encode_data(team) for team in data['response'])
            to_find = 'fixtures'
            teams_list = [row['team']['id'] for row in data['response']]

        # find all fixtures played in each league in each season
        elif endpoint == 'fixtures':
            total_fixtures_data.extend(encode_data(fix) for fix in data['response'])
            
            # if last endpoint is fixtures now find stats for every team in that season
            to_find = 'teams/statistics'
            
        # find all stats for each teams played in each league in each season
        else:
            total_team_stats_data.append(encode_data(data['response']))
            
            # drop team that data was already collected for
            if len(teams_list) > 0:
                teams_list.pop(0)

            # if team stats were collected for every team in current season move to next season
            if (len(teams_list) == 0) & (index < len(european_seasons)-1):
                index += 1
                league = european_seasons.loc[index]['league_id']
                year = european_seasons.loc[index]['year']
                to_find = 'teams'

            # if data for all seasons were collected quit loop
            else:
                done = True
                to_find = 'fixtures/statistics'
            
        remaining = int(remaining_req)
        print(remaining)
        # sleep cause there can be done only 10 requests per minute
        time.sleep(7)

    teams_path = '/opt/airflow/data/teams.json'
    with open(teams_path, 'w') as file:
        json.dump(teams_list, file)

    if not done:
        print(f'{len(european_seasons)-index-1} seasons left')
    else:
        print('Teams and their statistics were collected for every season played in Europe')

    # if all teams, teams statistics and fixtures were collected get fixtures stats
    if to_find == 'fixtures/statistics':
        total_fix_stats_data = []
        endpoint = to_find

        matches_path = '/opt/airflow/data/matches.json'
        with open(matches_path, 'r') as file:
            matches = json.load(file)

        # if there are no fixtures ids in list get fixtures ids from fixtures table
        if len(matches) == 0:
            matches = get_matches()
        # if all fixtures stats were collected list contains only 'done'
        if matches[0] == 'done':
            print('matches statistics collected')
        else:
            while (len(matches) > 0) & (remaining > 0):
                fixture = matches[0]
                params = {'fixture': fixture}
                df, remaining = get_data(endpoint, params)
                total_fix_stats_data.extend(encode_fix_stats(row, fixture) for row in df['response'])
                matches.pop(0)
            if len(matches) == 0:
                matches[0] = 'done'

            with open(matches_path, 'w') as file:
                json.dump(teams_list, file)
    # save current file to csv file
    data = {'league_id': [league], 'year': [year], 'type': [to_find]}
    current = pd.DataFrame(data)
    current.to_csv(current_path)

    return total_teams_data, total_fix_stats_data, total_fixtures_data, total_team_stats_data

# send collected data to PostgreSQL dbs
def send_to_postgresql_historical_data(teams_data, team_stats_data, fixtures_data, fix_stats_data):
    
    db_params = {
        'host': 'localhost',
        'database': 'preds',
        'user': 'postgres',
        'password': 'pass',
        'port': '5432'
    }

    if len(team_stats_data) > 0:
        team_stats_df = pd.DataFrame(team_stats_data)
        team_stats_df = team_stats_df.drop(columns = {
            'league_name', 
            'league_country', 
            'league_logo', 
            'league_flag', 
            'team_name', 
            'team_logo',
            'lineups'})
        team_stats_df = team_stats_df.fillna(0)
        conflict_col = []
        team_stats_df.columns = team_stats_df.columns.str.replace('-','_')
        data_to_sql('team_stats', team_stats_df, db_params, conflict_col)
        print('team stats data send')

    if len(teams_data) > 0:
        teams_df = pd.DataFrame(teams_data)
        teams_df = teams_df[['team_id', 
                            'team_name', 
                            'team_country', 
                            'team_logo', 
                            'team_national', 
                            'venue_capacity', 
                            'venue_surface']].rename(columns = {'team_national': 'national'})
        conflict_col = ['team_id']
        data_to_sql('teams', teams_df, db_params, conflict_col)
        print('team data send')

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
        conflict_col = ['fixture_id']
        data_to_sql('fixtures', fixtures_df, db_params, conflict_col)
        print('fixtures data send')

    if len(fix_stats_data) > 0:
        fixture_stats_df = pd.DataFrame(fix_stats_data)
        conflict_col = ['fixture_id', 'team_id']
        data_to_sql('fixture_statistics', fixture_stats_df, db_params, conflict_col)
        print('fixtures stats data send')
    
