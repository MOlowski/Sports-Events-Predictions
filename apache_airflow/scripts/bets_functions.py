import psycopg2
import pandas as pd
from datetime import date, timedelta

def get_odds_predictions(start, end):

    conn = None
    cur = None
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    try:
        # Establish the connection
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        query = """
        SELECT a.*
        FROM (
            SELECT p.*, o.result_home, o.result_draw, o.result_away, 
            o.both_scores_true, o.both_scores_false, o.double_chance_home, 
            o.double_chance_away, o.fh_result_home, o.fh_result_draw, o.fh_result_away, 
            o.home_over_1, o.home_over_2, o.away_over_1, o.away_over_2
            FROM odds o
            LEFT JOIN predictions p ON p.fixture_id = o.fixture_id
        ) a
        LEFT JOIN fixtures f ON a.fixture_id = f.fixture_id
        WHERE f.fixture_date >= %s and f.fixture_date <= %s
        """
        df = pd.read_sql_query(query, conn, params=(start, end))
        # Commit the changes
        conn.commit()
        return df
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

def preprocess_data(bets):
    cols=list(bets.columns)
    bets = bets.assign(bet_name=cols[2])
    bets = bets.rename(columns={cols[1]:'probability', cols[2]:'bet_value'})
    return bets

def bet_one_type(df, el, chance, bet, min_chance, bet_num):
    
    cols = ['fixture_id']+el
    df = df[cols] # get subset of data with one odd type
    df, new_el = get_df(el, df, min_chance)
    if len(df) > 0:
        summed_bet = 1
        summed_chance = 1 
        res_chance = 1 # final chance value
        res_bet = 1 # final bet value
        df.sort_values(by=el[0], ascending=False, inplace=True) # sort from best chance to worst
        tmp_df = df # get tmp df to drop rows with too low bet value if necessary
        tmp2_df = df # tmp2 for dropping chosen bets from df and find anothers without them
        final_bets = None
        chosen_bets = None
        while len(tmp_df) > 0:
            for num in range(len(tmp_df)):
                if len(el)==2:
                    summed_chance *= tmp_df[el[0]].iloc[num]
                    summed_bet *= tmp_df[el[1]].iloc[num]
                elif len(el)==3:
                    if tmp_df[el[0]].iloc[num] > min_chance:
                        summed_chance *= tmp_df[el[0]].iloc[num]
                    else:
                        summed_chance *= 1-tmp_df[el[0]].iloc[num]
                    summed_bet *= tmp_df[new_el].iloc[num]
                else:
                    summed_chance *= tmp_df[el[0]].iloc[num]
                    summed_bet *= tmp_df[new_el].iloc[num]
                
                if num==len(tmp_df)-1 and summed_chance > chance: 
                    # if its last iterate and inreaced chance is higher than given chance assign final values
                    res_chance = summed_chance
                    res_bet = summed_bet
                    
                if summed_chance < chance or num > 4 or num==len(tmp_df)-1:
                    if res_bet > bet: # if final bet value is okay save bet and remove used bets from df
                        if num==len(tmp_df)-1 and summed_chance > chance:
                            num += 1
                        chosen_bets = tmp_df.iloc[0:num]
                        chosen_bets = chosen_bets.assign(bet_number = bet_num)
                        bet_num+=1
                        if final_bets is None:
                            final_bets = chosen_bets
                        else:
                            final_bets = pd.concat([final_bets,chosen_bets])
                        to_drop = []
                        for n in range(num):
                            val1 = tmp_df.iloc[n].iloc[0]
                            val2 = tmp_df.iloc[n].iloc[1]
                            val3 = tmp_df.iloc[n].iloc[2]
                            if len(el)==2:
                                cond = (
                                    tmp2_df['fixture_id']==val1
                                )&(
                                    tmp2_df[el[0]]==val2
                                )&(
                                    tmp2_df[el[1]]==val3
                                )
                                
                            else:
                                cond = (
                                    tmp2_df['fixture_id']==val1
                                )&(
                                    tmp2_df[el[0]]==val2
                                )&(
                                    tmp2_df[new_el]==val3
                                )
                                
                            idx = tmp2_df[cond].index[0]
                            
                            to_drop.append(idx)
                        tmp2_df = tmp2_df.drop(to_drop)
                        tmp_df = tmp2_df

                    else: # if final bet value is too low drop row with the lowest bet 
                        if len(el)==2:
                            to_drop = tmp_df[el[1]].idxmin()
                        else:
                            to_drop = tmp_df[new_el].idxmin()
                        tmp_df = tmp_df.drop(to_drop)
                    summed_bet = 1
                    summed_chance = 1 
                    res_chance = 1
                    res_bet = 1
                    break
                else:
                    res_chance = summed_chance
                    res_bet = summed_bet
        if final_bets is not None:
            print(f'got {el[0]} bets')
            return final_bets, bet_num
        else:
            print(f'no odds for {el[1]} bet type')
            return None, bet_num
    else:
        print(f'no proper odds for {el[1]} bet type')
        return None, bet_num

def get_df(el, df, min_chance):
    if len(el)==2:
        df = df[df[el[0]]>min_chance] # get only odds with chance above 0.5
        new_el = ''
        
    elif len(el)==3:
        df = df[(df[el[0]]>min_chance)|(df[el[0]]<1-min_chance)] # get only odds with chance above minimal chance
        df['both_scores'] = df.apply(lambda row: row[el[1]] if row[el[0]]>0.5 else row[el[2]], axis=1)
        df = df.drop(columns={el[1],el[2]})
        new_el = 'both_scores'

    else:
        df = df[df[el[0]]>min_chance] # get only odds with chance above minimal chance
        if el[3].startswith('fh'):
            new_col='fh_result'
        else:
            new_col='result'
        df[new_col] = df.apply(
            lambda row: row[el[2]] if row[el[1]]==0 else (row[el[3]] if row[el[1]]==1 else row[el[4]]),
            axis=1
        )
        df = df.drop(columns={el[1],el[2],el[3],el[4]})
        new_el = new_col
    return df, new_el

def get_idx():
    conn = None
    cur = None
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    try:
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        query = '''
        SELECT MAX(bet_number) AS max_bet_number
        FROM bets
        '''
        cur.execute(query)
        res = cur.fetchone()
        if res[0]:
            return res[0]
        else:
            return 1
    except Exception as e:
        print(f'{e}')
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

def get_proper_odds():

    sets = [['home_over_1_prob','home_over_1'],
            ['home_over_2_prob','home_over_2'],
            ['away_over_1_prob','away_over_1'],
            ['away_over_2_prob','away_over_2'],
            ['both_scores_prob','both_scores_true','both_scores_false'],
            ['result_prob','result_pred','result_home', 'result_draw', 'result_away'],
            ['result_first_half_prob','result_first_half_pred','fh_result_home', 'fh_result_draw', 'fh_result_away'],
            ['result_double_chance_home_prob','double_chance_home'],
            ['result_double_chance_away_prob','double_chance_away']]
    min_odd_chance = 0.65
    min_final_chance = 0.6
    bonus_incomes = 0.2
    tax_value = 0.23
    min_final_bet_value = (1/min_final_chance + bonus_incomes/min_final_chance)
    min_final_bet_value = min_final_bet_value/(1-tax_value) # added tax value
    
    start = date.today()
    while start.weekday()!=4:
        start += timedelta(1)
    end = date.today()
    while end.weekday()!=0:
        end += timedelta(1)

    df = get_odds_predictions(start,end)

    bet_num = get_idx()
    final_bets = None

    for el in sets:
        bets, bet_num = bet_one_type(df,el, min_odd_chance, min_final_bet_value, min_final_chance, bet_num)
        if bets is not None:
            bets = preprocess_data(bets)
            if final_bets is not None:
                final_bets=pd.concat([final_bets,bets])
            else:
                final_bets = bets
        # preprocess data 
            
        # save bets to bets table
    final_df = pd.DataFrame(final_bets)
    return(final_df)

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
        """.format('bets', ','.join(df.columns), ','.join(['%s']*len(df.columns)), ','.join(conflict_columns))

        cur.executemany(insert_query, df.values.tolist())
        
        # Commit the changes
        conn.commit()
        return print(f'table bets updated')
    except Exception as e:
        print(f'Error {e}')
    
    finally:
        if conn is not None:
            # Close the cursor and connection
            cur.close()
        if cur is not None:
            conn.close()