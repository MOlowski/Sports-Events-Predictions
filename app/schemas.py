from pydantic import BaseModel
from datetime import date

class BetBase(BaseModel):
    fixture_id: int
    bet_prob: float
    bet_value: float
    bet_name: str
    bet_number: int

class Bet(BetBase):
    id: int

    class Config:
        orm_mode: True

class FixtureBase(BaseModel):
    fixture_id: int
    fixture_date: date

class Fixture(FixtureBase):
    bets: list[Bet] = []

    class Config:
        orm_mode: True

class FixtureUpdated(Base):
    __tablename__ = 'fixtures_updated'

    fixture_id = Column(Integer, ForeignKey('fixtures.fixture_id'))
    fixture_date = Column(Date, nullable=False)
    fixture_status_long = Column(String)
    fixture_status_short = Column(String)
    fixture_status_elapsed = Column(String)
    league_id = Column(Integer)
    league_season = Column(String)
    league_round = Column(String)
    teams_home_id = Column(Integer)
    teams_home_winner = Column(String)
    teams_away_id = Column(Integer)
    teams_away_winner = Column(String)
    goals_home = Column(Integer)
    goals_away = Column(Integer)
    score_halftime_home = Column(Integer)
    score_halftime_away = Column(Integer)
    score_fulltime_home = Column(Integer)
    score_fulltime_away = Column(Integer)
    teams_home_goals_scored_home = Column(Integer)
    teams_away_goals_scored_away = Column(Integer)
    teams_home_goals_lost_home = Column(Integer)
    teams_away_goals_lost_away = Column(Integer)
    teams_home_total_goals_scored = Column(Integer)
    teams_home_total_goals_lost = Column(Integer)
    teams_home_points = Column(Integer)
    teams_home_total_points = Column(Integer)
    teams_home_standings = Column(Integer)
    teams_home_last_five_matches_points = Column(Integer)
    teams_away_total_goals_scored = Column(Integer)
    teams_away_total_goals_lost = Column(Integer)
    teams_away_points = Column(Integer)
    teams_away_total_points = Column(Integer)
    teams_away_standings = Column(Integer)
    teams_away_last_five_matches_points = Column(Integer)