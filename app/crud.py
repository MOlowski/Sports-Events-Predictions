from fastapi import Depends, APIRouter, HTTPException
from sqlalchemy.future import select
from .models import Bet, Fixture, BetResult, Team, League
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from datetime import date
from . import crud, schemas, database
import pandas as pd

async def get_all_bets(db: AsyncSession):
    result = await db.execute(select(Bet))
    return result.scalars().all()

async def get_all_bets_results(db: AsyncSession):
    result = await db.execute(
        select(
            BetResult.fixture_id,
            BetResult.bet_name,
            BetResult.bet_number,
            BetResult.match_result,
            BetResult.bet_result,
            Bet.bet_value
        )
        .join(Bet, and_(Bet.bet_name == BetResult.bet_name, Bet.fixture_id == BetResult.fixture_id))
    )
    rows = result.fetchall()
    
    # Convert rows to a list of dictionaries
    bets = [
        {
            "fixture_id": row[0],
            "bet_name": row[1],
            "bet_number": row[2],
            "match_result": row[3],
            "bet_result": row[4],
            "bet_value": row[5]
        }
        for row in rows
    ]
    
    # Check for duplicates in the fetched results
    unique_bets = {f"{bet['bet_number']}_{bet['bet_name']}": bet for bet in bets}
    
    return list(unique_bets.values())

async def get_bets_by_fixture_and_date(db: AsyncSession, start_date:date, end_date:date):
    result = await db.execute(
        select(Bet, Fixture.fixture_date)
        .join(Fixture)
        .where(Fixture.fixture_date >= start_date)
        .where(Fixture.fixture_date <= end_date)
        .options(joinedload(Bet.fixture))
    )
    bets = result.scalars().all()
    return [bet.__dict__ for bet in bets]

async def get_bets_results_by_fixture_and_date(db: AsyncSession, start_date:date, end_date:date):
    result = await db.execute(
        select(
            BetResult.fixture_id,
            BetResult.bet_name,
            BetResult.bet_number,
            BetResult.match_result,
            BetResult.bet_result,
            Bet.bet_value
        )
        .join(Bet, and_(Bet.bet_name == BetResult.bet_name, Bet.fixture_id == BetResult.fixture_id))
        .join(Fixture, Bet.fixture_id == Fixture.fixture_id)
        .where(Fixture.fixture_date >= start_date)
        .where(Fixture.fixture_date <= end_date)
    )

    rows = result.fetchall()
    
    # Convert rows to a list of dictionaries
    bets = [
        {
            "fixture_id": row[0],
            "bet_name": row[1],
            "bet_number": row[2],
            "match_result": row[3],
            "bet_result": row[4],
            "bet_value": row[5]
        }
        for row in rows
    ]
    
    # Check for duplicates in the fetched results
    unique_bets = {f"{bet['bet_number']}_{bet['bet_name']}": bet for bet in bets}
    
    return list(unique_bets.values())

async def get_bet_by_number(db: AsyncSession, bet_number: int):
    result = await db.execute(
        select(
            Bet, 
            Fixture.fixture_date, 
            League.name.label('league_name'), 
            League.country.label('league_country'), 
            Team.team_name.label('team_home_name')
        )
        .join(Fixture, Bet.fixture_id == Fixture.fixture_id)
        .join(League, Fixture.league_id == League.league_id)
        .join(Team, Fixture.teams_home_id == Team.team_id)
        .where(Bet.bet_number == bet_number)
        .options(joinedload(Bet.fixture))  # Assuming you want to eager load `Bet.fixture`
    )
    bets = result.fetchall()
    return [
        {
            **bet.Bet.__dict__,
            "fixture_date": bet.fixture_date,
            "league_name": bet.league_name, 
            "league_country": bet.league_country,
            "team_home_name": bet.team_home_name
        }
        for bet in bets
    ]

async def calculate_acc(bets):
    df = pd.DataFrame(bets)
    print(len(df), df[['fixture_id','bet_name','bet_result','bet_number']].head(15))
    df1 = df.groupby('bet_number')['bet_value'].prod().reset_index()
    df1 = df1.rename(columns={'bet_value':'summed_val'})
    df2 = df.merge(df1, on='bet_number')
    df3 = df2.drop_duplicates(subset=['bet_number'])
    acc_by_bet_name = df3.groupby(['bet_name', 'bet_result']).size().reset_index(name='num')
    match_acc_by_bet_name = df2.groupby(['bet_name', 'match_result']).size().reset_index(name='num')
    df3['cash'] = df3.apply(lambda row: (row['summed_val']*10)-10 if row['bet_result'] else -10, axis=1)
    incomes = df3.groupby('bet_name')['cash'].sum()
    return acc_by_bet_name, match_acc_by_bet_name, incomes