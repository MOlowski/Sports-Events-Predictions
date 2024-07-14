from sqlalchemy.future import select
from .models import Bet, Fixture
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date

async def get_all_bets(db: AsyncSession):
    result = await db.execute(select(Bet))
    return result.scalars().all()


async def get_bets_by_fixture_and_date(db: AsyncSession, start_date: date, end_date:date):
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
