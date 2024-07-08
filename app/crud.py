from sqlalchemy.orm import Session
from . import models, schemas
from datetime import date, timedelta

def get_bets_last_weekend(db: Session):
    today = date.today()
    start = today
    while start.weekday() != 4:
        start -= timedelta(days=1)
    end = start
    while end.weekday() != 0:
        end += timedelta(days=1)

    return db.query(models.Bet).join(models.Fixture).filter(
        and_(
            models.Fixture.date >= start.strftime("%Y-%m-%d"),
            models.Fixture.date <= end.strftime("%Y-%m-%d"),
        )
    ).all()

def get_bet(db:Session, bet_id: int):
    return db.query(models.Bet).filter(models.Bet.id == bet_id).first()


# Define CRUD operations for Fixture, Team, and League similarly