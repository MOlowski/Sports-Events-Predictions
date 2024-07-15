from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from datetime import date, timedelta
from .. import crud, schemas, models, database

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

@router.get("/{bet_id}", response_model=List[schemas.Bet])
async def read_bets_by_bet_number(bet_id: int, db: AsyncSession = Depends(database.get_db)):
    bet = await db.get(models.Bet, bet_id)
    if not bet:
        raise HTTPException(status_code=404, detail="Bet not found")
    bets = await crud.get_bets_by_bet_number(db, bet.bet_number)
    return bets

@router.get("/info", response_class=StreamingResponse)
async def plot_combined_info(db: AsyncSession = Depends(database.get_db)):
    start_date, end_date = get_dates()
    bets_last_weekend = await crud.get_bets_by_date_range(db, start_date, end_date)
    accuracy_by_bet_name_last_weekend, match_accuracy_by_bet_name_last_weekend = await crud.calculate_accuracy(bets_last_weekend)

    bets_all_time = await crud.get_all_bets(db)
    accuracy_by_bet_name_all_time, match_accuracy_by_bet_name_all_time = await crud.calculate_accuracy(bets_all_time)

    return await plots.get_combined_plot(accuracy_by_bet_name_last_weekend, match_accuracy_by_bet_name_last_weekend, accuracy_by_bet_name_all_time, match_accuracy_by_bet_name_all_time)

@router.get("/{bet_id}")
async def read_bet(bet_id: int, db: AsyncSession = Depends(database.get_db)):
    bet = await crud.get_bet_by_id(db, bet_id)
    if bet.fixture.fixture_updated.short_status != "NS":
        # Calculate values
        bet.home_goals_over_2 = True if bet.fixture.goals_home > 2 else False
        bet.home_goals_over_2 = True if bet.fixture.goals_home > 2 else False
        bet.away_goals_over_3 = True if bet.fixture.goals_away > 3 else False
        bet.away_goals_over_3 = True if bet.fixture.goals_away > 3 else False
        
    return bet