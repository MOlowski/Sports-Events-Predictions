from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from datetime import date, timedelta
from .. import crud, schemas, database

router = APIRouter()

def get_dates():
    t = date.today()
    start_date = t
    if start_date.weekday() < 5:
        while start_date.weekday() != 5:
            start_date -= timedelta(days=1)
    else:
        while start_date.weekday() != 5:
            start_date += timedelta(days=1)

    end_date = start_date
    while end_date.weekday() != 0:
        end_date += timedelta(days=1)

    return start_date, end_date


@router.get("/", response_model=List[schemas.Bet])
async def read_bets(db: AsyncSession = Depends(database.get_db)):
    start_date, end_date = get_dates()
    bets = await crud.get_bets_by_fixture_and_date(db, start_date, end_date)
    return bets

