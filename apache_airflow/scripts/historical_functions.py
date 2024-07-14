import requests
import numpy as np
import pandas as pd
import json
import os
import time
from airflow.hooks.postgres_hook import PostgresHook

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

# get list of matches to collect matches stats
def get_matches(pg_hook):
    query = '''
    SELECT fixture_id, league_id, league_season
    FROM fixtures
    WHERE fixture_status_short IN ('FT', 'WO', 'AET', 'PEN', 'CANC')
    '''
    conn = None
    
    try:
        conn = pg_hook.get_conn()

        df = pd.read_sql_query(query,conn)

        # filter if league has fixtures stats available
        s_path = '/opt/airflow/data/seasons.csv'
        seasons = pd.read_csv(s_path)
        valid_leagues_seasons = set(zip(seasons[seasons['statistics_fixtures'] == True]['league_id'], 
                                seasons[seasons['statistics_fixtures'] == True]['year']))
        
        res_df = df[df.apply(lambda row: (int(row['league_id']), int(row['league_season'])) in valid_leagues_seasons, axis=1)]
    
        return list(res_df['fixture_id'])
    except Exception as e:
        print(f'Error {e} occured')

    finally:
        if conn is not None:
            conn.close()

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
    print(index)
    remaining = 10000
    total_team_stats_data = []
    total_teams_data = []
    total_fixtures_data = []
    total_fix_stats_data = []

    if index == len(european_seasons)-1:
        done = True
    else:
        done = False

    # if script ended on collecting team stats get rest of teams
    if to_find == 'teams/statistics':
        teams_path = '/opt/airflow/data/teams.json'
        with open(teams_path, 'r') as file:
            teams_list = json.load(file)
        if (len(teams_list) == 0)&(not done):
            to_find = 'teams'
    else:
        teams_list = []

    if to_find == 'fixtures/statistics':
        done = True

    # if limit of requests wasnt reached and there is still something to collect enter loop
    while (done==False)&(remaining > 500):

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
            print(f'team {team} stats from {year} league {league}, {len(teams_list)} teams left')
        else:
            params = {
                'league': league,
                'season': year
                }
            print('fixtures')
        
        endpoint = to_find
        data, remaining_req = get_data(endpoint, params)

        if int(remaining_req)>0:

            # find all teams played in each league in each season
            if endpoint == 'teams':
                total_teams_data.extend(encode_data(team) for team in data['response'])
                to_find = 'fixtures'
                teams_list = [row['team']['id'] for row in data['response']]
            
            # find all fixtures played in each league in each season
            elif endpoint == 'fixtures':
                total_fixtures_data.extend(encode_data(fix) for fix in data['response'])
                
                # if last endpoint is fixtures now find stats for every team in that season
                if len(teams_list) > 0:
                    to_find = 'teams/statistics'
                else:
                    to_find = 'teams'
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
                if (len(teams_list) == 0) & (index >= len(european_seasons)-1):
                    done = True
                    to_find = 'fixtures/statistics'
            
        remaining = int(remaining_req)
        print(remaining)
        # sleep cause there can be done only 10 requests per minute
        time.sleep(0.2)

    teams_path = '/opt/airflow/data/teams.json'
    with open(teams_path, 'w') as file:
        json.dump(teams_list, file)

    if not done:
        print(index)
        print(f'{len(european_seasons)-index-1} seasons left')
    else:
        print('Teams and their statistics were collected for every season played in Europe')

    # if all teams, teams statistics and fixtures were collected get fixtures stats
    if to_find == 'fixtures/statistics':

        endpoint = to_find

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        matches_path = '/opt/airflow/data/matches.json'
        with open(matches_path, 'r') as file:
            matches = json.load(file)

        # if there are no fixtures ids in list get fixtures ids from fixtures table
        if len(matches) == 0:

            matches = get_matches(pg_hook)

        # if all fixtures stats were collected list contains only 'done'
        if matches[0] == 'done':

            future_engineering(pg_hook)
            print('matches statistics collected')
        else:
            while (len(matches) > 0) & (remaining > 500):
                fixture = matches[0]
                params = {'fixture': fixture}
                df, remaining_req = get_data(endpoint, params)
                total_fix_stats_data.extend(encode_fix_stats(row, fixture) for row in df['response'])
                matches.pop(0)
                print(matches[0], len(matches),'left')
                if len(matches) == 0:
                    matches[0] = 'done'
                remaining = int(remaining_req)
            with open(matches_path, 'w') as file:
                json.dump(matches, file)
    # save current file to csv file
    data = {'league_id': [league], 'year': [year], 'type': [to_find]}
    current = pd.DataFrame(data)
    current.to_csv(current_path)

    if len(total_team_stats_data) > 0:
        total_team_stats_data = pd.DataFrame(total_team_stats_data)
    if len(total_teams_data) > 0:
        total_teams_data = pd.DataFrame(total_teams_data)
    if len(total_fixtures_data) > 0:
        total_fixtures_data = pd.DataFrame(total_fixtures_data)
    if len(total_fix_stats_data) > 0:
        total_fix_stats_data = pd.DataFrame(total_fix_stats_data)

    return total_teams_data, total_team_stats_data, total_fixtures_data, total_fix_stats_data

# send collected data to PostgreSQL dbs
def send_to_postgresql_historical_data(teams_df, team_stats_df, fixtures_df, fixture_stats_df):
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    if len(teams_df) > 0:
        teams_df = teams_df[['team_id', 
                            'team_name', 
                            'team_country', 
                            'team_logo', 
                            'team_national', 
                            'venue_capacity', 
                            'venue_surface']].rename(columns = {'team_national': 'national'})
        conflict_col = ['team_id']
        teams_df = teams_df.fillna(0)
        data_to_sql('teams', teams_df, pg_hook, conflict_col)
        print('team data send')

    if len(team_stats_df) > 0:

        col_to_drop = [col for col in team_stats_df.columns if col.startswith('lineups')]

        columns_drop = [
            'league_name', 
            'league_country', 
            'league_logo', 
            'league_flag', 
            'team_name', 
            'team_logo',
            'cards_yellow__total',
            'cards_yellow__percentage',
            'cards_red__total',
            'cards_red__percentage'
        ] + col_to_drop
        team_stats_df = team_stats_df.drop(columns = columns_drop, errors='ignore')
        team_stats_df = team_stats_df.fillna(0)
        conflict_col = []
        team_stats_df.columns = team_stats_df.columns.str.replace('-','_')
        data_to_sql('team_stats', team_stats_df, pg_hook, conflict_col)
        print('team stats data send')

    if len(fixtures_df) > 0:
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
        data_to_sql('fixtures', fixtures_df, pg_hook, conflict_col)
        print('fixtures data send')

    if len(fixture_stats_df) > 0:
        fixture_stats_df = fixture_stats_df.fillna(0)
        if 'goals_prevented' in fixture_stats_df.columns:
            to_drop = ['cross_attacks', 'free_kicks', 'goals', 
                       'goal_attempts', 'substitutions', 'throwins', 
                       'medical_treatment', 'goals_prevented', 'assists', 
                       'counter_attacks']
            fixture_stats_df = fixture_stats_df.drop(columns=to_drop, errors='ignore')
            print(fixture_stats_df.columns)
        conflict_col = ['fixture_id', 'team_id']
        data_to_sql('fixture_statistics', fixture_stats_df, pg_hook, conflict_col)
        print('fixtures stats data send')

# add statistics values to fixtures table (points, goals, standings)
def add_statistics(fixtures_df):
        
    fixtures_df['fixture_date'] = pd.to_datetime(fixtures_df['fixture_date']).dt.date
    fixtures_df = fixtures_df.sort_values(by='fixtures_date')
    fixtures_df['teams_home_goals_scored_home'] = fixtures_df.groupby(['league_season', 'team_home_id'])['goals_home'].apply(lambda x: x.shift().cumsum())
    fixtures_df['teams_home_goals_scored_away'] = fixtures_df.groupby(['league_season','team_away_id'])['goals_away'].apply(lambda x: x.shift().cumsum())
    fixtures_df['teams_away_goals_lost_home'] = fixtures_df.groupby(['league_season','team_home_id'])['goals_away'].apply(lambda x: x.shift().cumsum())
    fixtures_df['teams_away_goals_lost_away'] = fixtures_df.groupby(['league_season','team_away_id'])['goals_home'].apply(lambda x: x.shift().cumsum())
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
    total['total_goals_scored'] = total[['fixture_date','league_season','team_id','goals_scored']].groupby(['league_season','team_id'])['goals_scored'].apply(lambda x: x.shift().cumsum())
    total['total_goals_lost'] = total[['fixture_date','league_season','team_id','goals_lost']].groupby(['league_season','team_id'])['goals_lost'].apply(lambda x: x.shift().cumsum())
    
    total = total.sort_values(by='fixture_date')
    total['total_points'] = total[['fixture_date', 'league_season', 'team_id', 'league_round', 'points']].groupby(['league_season','team_id'])['points'].apply(lambda x: x.shift().cumsum())

    total.sort_values(by=['league_season','league_round','total_points','total_goals_scored','fixture_date'], ascending=[True,True,False,False,True])
    total['standings'] = total.groupby(['league_season','league_round'])['total_points'].rank(method='min', ascending=False)
    total['standings'] = total['standings'].astype(int)

    total = total.sort_values(by=['team_id','fixture_date'])
    total['points_last_5_matches'] = total.groupby('team_id')['points'].aply(lambda x: x.shift().rolling(window=5, min_periods=1).sum())
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

# add stats to fixtures table and save it in fixtures update table
def future_engineering(pg_hook):
    conn = None
    cur = None
    fixtures_df = None
    conflict_columns = ['fixture_id']
    try:
        conn = pg_hook.get_conn()

        query = '''
        SELECT *
        FROM fixtures
        '''
        
        fixtures_df = pd.read_sql_query(query, conn)
        
        df = add_statistics(fixtures_df)

        insert_query = """
            INSERT INTO {} ({})
            VALUES ({})
            ON CONFLICT ({}) DO NOTHING
        """.format('fixtures_updated', ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns))

        cur.executemany(insert_query, df.values.tolist())
        print(f'table fixtures_updated updated')
        
        # Commit the changes
        conn.commit()

    except Exception as e:
        print(f'Error {e}')
    
    finally:
        if conn is not None:
            # Close the cursor and connection
            cur.close()
        if cur is not None:
            conn.close()

