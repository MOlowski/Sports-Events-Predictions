from datetime import date
import pandas as pd
import json
import requests
from airflow.hooks.postgres_hook import PostgresHook

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

    if today.weekday() > 1 & len(last) == 0:
        done = True

    while (remaining > 0)&(done == False):
        params = {'league' : to_get[0][0], 'year': to_get[0][1]}
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

    if len(fix_data) > 0:
        update_cols = list(df.columns)
        update_cols = update_cols - ['fixture_id']
        data_to_sql('fixtures', df, 'fixture_id', update_cols)