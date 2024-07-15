from fastapi import Depends, APIRouter, HTTPException
from sqlalchemy.future import select
from .models import Bet, Fixture, FixtureUpdated
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date
from . import crud, schemas, database

async def get_all_bets(db: AsyncSession):
    result = await db.execute(select(Bet))
    return result.scalars().all()

async def get_bets_by_fixture_and_date( start_date:date, end_date:date, db: AsyncSession):
    result = await db.execute(
        select(Bet)
        .join(Fixture)
        .filter(
            Fixture.fixture_date >= start_date,
            Fixture.fixture_date <= end_date
        )
    )
    bets = result.scalars().all()
    return [bet.__dict__ for bet in bets]

async def get_bet_by_id(bet_id: int, db: AsyncSession):
    result = await db.execute(
        select(Bet)
        .join(Fixture)
        .join(FixtureUpdated)
        .filter(Bet.bet_number == bet_id)
    )
    bets = result.scalars().all()
    return [bet.__dict__ for bet in bets]

async def caculate_acc(bets):
    df = pd.DataFrame([bet.__dict__ for bet in bets])
    df['']