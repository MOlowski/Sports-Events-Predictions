import psycopg2
import pandas as pd
import datetime
from datetime import date
from airflow.hooks.postgres_hook import PostgresHook

def get_current_matches(pg_hook):
    
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
    WHERE fixture_date >= {} and fixture_date <= {} and fixture_status_short = 'NS'
    '''.format(start_date, end_date)
        matches = pd.read_sql_query(query, conn)

        return matches
    except Exception as e:
        print(f'Error {e}')
        return None
    finally:
        if conn is not None:
            conn.close()

def predict(df):
    # predict some results
    predictions = None
    # send predictions to db
    return predictions


# function getting data about predicted matches and sending to db
def get_played_matches():
    