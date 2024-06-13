from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from scripts.historical_functions import get_and_preproc_historical_data, send_to_postgresql_historical_data
from airflow.hooks.postgres_hook import PostgresHook
import random
import string
from psycopg2 import sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'insert_data_dag',
    default_args = default_args,
    description = 'DAG testing postgres connection',
    schedule_interval = '@daily',
    start_date = days_ago(1),
    catchup = False,
)

def create_test_table():

    conn = None
    cur = None

    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cur = conn.cursor()

    # Define the table creation query

        create_countries_table = """
        CREATE TABLE IF NOT EXISTS countries (
            name VARCHAR(255) PRIMARY KEY,
            code VARCHAR(10),
            logo VARCHAR(255)
        );
        """

        create_leagues_table = """
        CREATE TABLE IF NOT EXISTS leagues (
            league_id INT PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(50),
            logo VARCHAR(255),
            country VARCHAR(255),
            country_code VARCHAR(10),
            seasons INT,
            CONSTRAINT fk_league_country
                FOREIGN KEY (country) REFERENCES countries(name)
        );
        """

        create_teams_table = """
        CREATE TABLE IF NOT EXISTS teams (
            team_id INT PRIMARY KEY,
            team_name VARCHAR(255),
            team_country VARCHAR(255),
            team_logo VARCHAR(255),
            national BOOLEAN,
            vanue_id INT,
            venue_capacity INT,
            venue_surface VARCHAR(255),
            CONSTRAINT fk_team_country
                FOREIGN KEY (team_country) REFERENCES countries(name)
        );
        """

        create_team_stats_table = """
        CREATE TABLE IF NOT EXISTS team_stats (
            league_id INTEGER,
            league_season INTEGER,
            team_id INTEGER,
            form VARCHAR(255),
            fixtures_played_home INTEGER,
            fixtures_played_away INTEGER,
            fixtures_played_total INTEGER,
            fixtures_wins_home INTEGER,
            fixtures_wins_away INTEGER,
            fixtures_wins_total INTEGER,
            fixtures_draws_home INTEGER,
            fixtures_draws_away INTEGER,
            fixtures_draws_total INTEGER,
            fixtures_loses_home INTEGER,
            fixtures_loses_away INTEGER,
            fixtures_loses_total INTEGER,
            goals_for_total_home INTEGER,
            goals_for_total_away INTEGER,
            goals_for_total_total INTEGER,
            goals_for_average_home FLOAT,
            goals_for_average_away FLOAT,
            goals_for_average_total FLOAT,
            goals_for_minute_0_15_total INTEGER,
            goals_for_minute_0_15_percentage VARCHAR(255),
            goals_for_minute_16_30_total INTEGER,
            goals_for_minute_16_30_percentage VARCHAR(255),
            goals_for_minute_31_45_total INTEGER,
            goals_for_minute_31_45_percentage VARCHAR(255),
            goals_for_minute_46_60_total INTEGER,
            goals_for_minute_46_60_percentage VARCHAR(255),
            goals_for_minute_61_75_total INTEGER,
            goals_for_minute_61_75_percentage VARCHAR(255),
            goals_for_minute_76_90_total INTEGER,
            goals_for_minute_76_90_percentage VARCHAR(255),
            goals_for_minute_91_105_total INTEGER,
            goals_for_minute_91_105_percentage VARCHAR(255),
            goals_for_minute_106_120_total INTEGER,
            goals_for_minute_106_120_percentage VARCHAR(255),
            goals_against_total_home INTEGER,
            goals_against_total_away INTEGER,
            goals_against_total_total INTEGER,
            goals_against_average_home FLOAT,
            goals_against_average_away FLOAT,
            goals_against_average_total FLOAT,
            goals_against_minute_0_15_total INTEGER,
            goals_against_minute_0_15_percentage VARCHAR(255),
            goals_against_minute_16_30_total INTEGER,
            goals_against_minute_16_30_percentage VARCHAR(255),
            goals_against_minute_31_45_total INTEGER,
            goals_against_minute_31_45_percentage VARCHAR(255),
            goals_against_minute_46_60_total INTEGER,
            goals_against_minute_46_60_percentage VARCHAR(255),
            goals_against_minute_61_75_total INTEGER,
            goals_against_minute_61_75_percentage VARCHAR(255),
            goals_against_minute_76_90_total INTEGER,
            goals_against_minute_76_90_percentage VARCHAR(255),
            goals_against_minute_91_105_total INTEGER,
            goals_against_minute_91_105_percentage VARCHAR(255),
            goals_against_minute_106_120_total INTEGER,
            goals_against_minute_106_120_percentage VARCHAR(255),
            biggest_streak_wins INTEGER,
            biggest_streak_draws INTEGER,
            biggest_streak_loses INTEGER,
            biggest_wins_home VARCHAR(255),
            biggest_wins_away VARCHAR(255),
            biggest_loses_home VARCHAR(255),
            biggest_loses_away VARCHAR(255),
            biggest_goals_for_home INTEGER,
            biggest_goals_for_away INTEGER,
            biggest_goals_against_home INTEGER,
            biggest_goals_against_away INTEGER,
            clean_sheet_home INTEGER,
            clean_sheet_away INTEGER,
            clean_sheet_total INTEGER,
            failed_to_score_home INTEGER,
            failed_to_score_away INTEGER,
            failed_to_score_total INTEGER,
            penalty_scored_total INTEGER,
            penalty_scored_percentage VARCHAR(255),
            penalty_missed_total INTEGER,
            penalty_missed_percentage VARCHAR(255),
            penalty_total INTEGER,
            cards_yellow_0_15_total INTEGER,
            cards_yellow_0_15_percentage VARCHAR(255),
            cards_yellow_16_30_total INTEGER,
            cards_yellow_16_30_percentage VARCHAR(255),
            cards_yellow_31_45_total INTEGER,
            cards_yellow_31_45_percentage VARCHAR(255),
            cards_yellow_46_60_total INTEGER,
            cards_yellow_46_60_percentage VARCHAR(255),
            cards_yellow_61_75_total INTEGER,
            cards_yellow_61_75_percentage VARCHAR(255),
            cards_yellow_76_90_total INTEGER,
            cards_yellow_76_90_percentage VARCHAR(255),
            cards_yellow_91_105_total INTEGER,
            cards_yellow_91_105_percentage VARCHAR(255),
            cards_yellow_106_120_total INTEGER,
            cards_yellow_106_120_percentage VARCHAR(255),
            cards_red_0_15_total INTEGER,
            cards_red_0_15_percentage VARCHAR(255),
            cards_red_16_30_total INTEGER,
            cards_red_16_30_percentage VARCHAR(255),
            cards_red_31_45_total INTEGER,
            cards_red_31_45_percentage VARCHAR(255),
            cards_red_46_60_total INTEGER,
            cards_red_46_60_percentage VARCHAR(255),
            cards_red_61_75_total INTEGER,
            cards_red_61_75_percentage VARCHAR(255),
            cards_red_76_90_total INTEGER,
            cards_red_76_90_percentage VARCHAR(255),
            cards_red_91_105_total INTEGER,
            cards_red_91_105_percentage VARCHAR(255),
            cards_red_106_120_total INTEGER,
            cards_red_106_120_percentage VARCHAR(255),
            CONSTRAINT fk_stats_league
                FOREIGN KEY (league_id) REFERENCES leagues(league_id),
            CONSTRAINT fk_stats_team
                FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );
        """

        create_players_table = """
        CREATE TABLE IF NOT EXISTS players (
            player_id INT PRIMARY KEY,
            name VARCHAR(255),
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            age INT,
            birth DATE,
            country VARCHAR(255),
            nationality VARCHAR(255),
            height VARCHAR(255),
            weight VARCHAR(255),
            injured BOOLEAN,
            photo VARCHAR(255),
            current_team INT,
            CONSTRAINT fk_players_countries
                FOREIGN KEY (country) REFERENCES countries(name)
        );
        """

        create_player_stats_table = """
        CREATE TABLE IF NOT EXISTS player_stats (
            player_id INT PRIMARY KEY,
            injured BOOLEAN,
            team_id INT,
            season INT,
            appearances INT,
            lineups INT,
            minutes INT,
            position VARCHAR(255),
            rating FLOAT,
            captain BOOLEAN,
            substitutes_in INT,
            substitutes_out INT,
            substitutes_bench INT,
            shots_total INT,
            shots_on INT,
            goals_total INT,
            goals_conceded INT,
            goals_assists INT,
            goals_saves INT,
            passes_total INT,
            passes_key INT,
            passes_accuracy FLOAT,
            tackles_total INT,
            tackles_blocks INT,
            tackles_interceptions INT,
            duels_total INT,
            duels_won INT,
            dribbles_attempts INT,
            dribbles_success INT,
            dribbles_past INT,
            fouls_drawn INT,
            fouls_committed INT,
            cards_yellow INT,
            cards_yellowred INT,
            cards_red INT,
            penalty_won INT,
            penalty_committed INT,
            penalty_scored INT,
            penalty_missed INT,
            penalty_saved INT,
            CONSTRAINT fk_player_stats_teams
                FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );
        """

        create_fixtures_table = """
        CREATE TABLE IF NOT EXISTS fixtures (
            fixture_id SERIAL PRIMARY KEY,
            fixture_referee VARCHAR(255),
            fixture_date DATE,
            fixture_venue_id INT,
            fixture_status_long VARCHAR(255),
            fixture_status_short VARCHAR(50),
            fixture_status_elapsed INT,
            league_id INT,
            league_season VARCHAR(50),
            league_round VARCHAR(255),
            teams_home_id INT,
            teams_home_winner BOOLEAN,
            teams_away_id INT,
            teams_away_winner BOOLEAN,
            goals_home INT,
            goals_away INT,
            score_halftime_home INT,
            score_halftime_away INT,
            score_fulltime_home INT,
            score_fulltime_away INT,
            score_extratime_home INT,
            score_extratime_away INT,
            score_penalty_home INT,
            score_penalty_away INT,
            CONSTRAINT fk_fixture_team_home_id
                FOREIGN KEY (teams_home_id) REFERENCES teams(team_id),
            CONSTRAINT fk_fixture_team_away_id
                FOREIGN KEY (teams_away_id) REFERENCES teams(team_id)
        );
        """

        create_fixture_statistics_table = """
        CREATE TABLE IF NOT EXISTS fixture_statistics (
            id SERIAL PRIMARY KEY,
            team_id INTEGER,
            shots_on_goal INTEGER,
            shots_off_goal INTEGER,
            total_shots INTEGER,
            blocked_shots INTEGER,
            shots_insidebox INTEGER,
            shots_outsidebox INTEGER,
            fouls INTEGER,
            corner_kicks INTEGER,
            offsides INTEGER,
            ball_possession VARCHAR(10),
            yellow_cards INTEGER,
            red_cards INTEGER,
            goalkeeper_saves INTEGER,
            total_passes INTEGER,
            passes_accurate INTEGER,
            passes_percentage VARCHAR(10),
            expected_goals FLOAT,
            CONSTRAINT fk_fixture_stats_team
                FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );
        """

        create_fixtures_updated_table = """
        CREATE TABLE IF NOT EXISTS fixtures_updated (
            fixture_id SERIAL PRIMARY KEY,
            fixture_referee VARCHAR(255),
            fixture_date DATE,
            fixture_venue_id INT,
            fixture_status_long VARCHAR(255),
            fixture_status_short VARCHAR(50),
            fixture_status_elapsed INT,
            league_id INT,
            league_season VARCHAR(50),
            league_round VARCHAR(255),
            teams_home_id INT,
            teams_home_winner BOOLEAN,
            teams_away_id INT,
            teams_away_winner BOOLEAN,
            goals_home INT,
            goals_away INT,
            score_halftime_home INT,
            score_halftime_away INT,
            score_fulltime_home INT,
            score_fulltime_away INT,
            score_extratime_home INT,
            score_extratime_away INT,
            score_penalty_home INT,
            score_penalty_away INT,
            teams_home_total_goals_scored INT,
            teams_home_total_goals_lost INT,
            teams_home_points INT,
            teams_home_total_points INT,
            teams_home_standings INT,
            teams_home_last_five_matches_points INT,
            teams_away_total_goals_scored INT,
            teams_away_total_goals_lost INT,
            teams_away_points INT,
            teams_away_total_points INT,
            teams_away_standings INT,
            teams_away_last_five_matches_points INT,
            CONSTRAINT fk_fixture_team_home_id
                FOREIGN KEY (teams_home_id) REFERENCES teams(team_id),
            CONSTRAINT fk_fixture_team_away_id
                FOREIGN KEY (teams_away_id) REFERENCES teams(team_id)
        );
        """
        cur.execute(create_countries_table)
        print('countries table created')
        cur.execute(create_leagues_table)
        print('leagues table created')
        cur.execute(create_teams_table)
        print('teams table created')
        cur.execute(create_team_stats_table)
        print('team stats table created')
        cur.execute(create_fixtures_table)
        print('fixtures table created')
        cur.execute(create_fixture_statistics_table)
        print('fixture stats table created')
        cur.execute(create_players_table)
        print('players table created')
        cur.execute(create_player_stats_table)
        print('player stats table created')
        cur.execute(create_fixtures_updated_table)
        print('fixtures updated table created')        
        
        # Commit the changes
        query = '''
        CREATE TABLE IF NOT EXISTS test(
        id INT PRIMARY KEY,
        code VARCHAR(10),
        number INT
        );
        '''

        print("Tables created successfully")
        cur.execute(query)
        print('done')
        conn.commit()

        return print('all goood')

    except Exception as e:
        print(f'Error {e}')
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

def insert_val_test():

    conn = None
    cur = None

    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        query  = '''
INSERT INTO test (id, code, number) VALUES (%s, %s, %s)
'''
        
        for _ in range(10):
            id_v = random.randint(1,100)
            code_v = ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))
            number_v = random.randint(1,100)
            
            cur.execute(query, (id_v, code_v, number_v))
        conn.commit()

        return print('all goood')

    except Exception as e:
        print(f'Error {e}')
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    
create_test_table_task = PythonOperator(
    task_id = 'create_random_table',
    python_callable = create_test_table,
    dag = dag,
)

insert_val_test_task = PythonOperator(
    task_id = 'insert_val_test',
    python_callable = insert_val_test,
    dag = dag,
)

create_test_table_task >> insert_val_test_task