from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from datetime import date, timedelta
from .. import crud, schemas, models, database
from fastapi.responses import StreamingResponse
from .plots import plots

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

@router.get("/summary", response_class=StreamingResponse)
async def plot_combined_info(db: AsyncSession = Depends(database.get_db)):
    print('a')
    start_date, end_date = get_dates()
    bets_last_weekend = await crud.get_bets_results_by_fixture_and_date(db, start_date, end_date)
    accuracy_by_bet_name_last_weekend, match_accuracy_by_bet_name_last_weekend, incomes_last_weekend = await crud.calculate_acc(bets_last_weekend)
    
    bets_all_time = await crud.get_all_bets_results(db)
    accuracy_by_bet_name_all_time, match_accuracy_by_bet_name_all_time, incomes_all_time = await crud.calculate_acc(bets_all_time)
    print(incomes_all_time)
    return await plots.get_combined_plot(
        accuracy_by_bet_name_last_weekend, 
        match_accuracy_by_bet_name_last_weekend,
        incomes_last_weekend, 
        accuracy_by_bet_name_all_time, 
        match_accuracy_by_bet_name_all_time,
        incomes_all_time
        )

@router.get("/{bet_number}", response_model=List[schemas.Bet])
async def read_bets_by_bet_number(bet_number: int, db: AsyncSession = Depends(database.get_db)):

    bets = await crud.get_bet_by_number(db, bet_number)
    if not bets:
        raise HTTPException(status_code=404, detail="Bet not found")
    return bets



