from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from .. import crud, models, schemas, database
from typing import List

router = APIRouter()

@router.post("/bets/", response_model=List[schemas.Bet])
def get_bets_last_weekend(bet: schemas.Bet, db: Session = Depends(database.get_db)):
    bets = crud.get_bets_last_weekend(db)
    return bets

@router.get("/bets/{bet_id}", response_model=schemas.Bet)
def read_bet(bet_id: int, db: Session = Depends(database.get_db)):
    db_bet = crud.get_bet(db, bet_id=bet_id)
    if db_bet is None:
        raise HTTPException(status_code=404, detail="Bet not found")
    return db_bet