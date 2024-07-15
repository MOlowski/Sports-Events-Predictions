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

class FixtureUpdatedBase(BaseModel):
    __tablename__ = 'fixtures_updated'

    fixture_id: int
    fixture_date: date
    fixture_status_long : str
    fixture_status_short : str
    fixture_status_elapsed : str
    league_id: int
    league_season : str
    league_round : str
    teams_home_id: int
    teams_home_winner : str
    teams_away_id: int
    teams_away_winner : str
    goals_home: int
    goals_away: int
    score_halftime_home: int
    score_halftime_away: int
    score_fulltime_home: int
    score_fulltime_away: int
    teams_home_goals_scored_home: int
    teams_away_goals_scored_away: int
    teams_home_goals_lost_home: int
    teams_away_goals_lost_away: int
    teams_home_total_goals_scored: int
    teams_home_total_goals_lost: int
    teams_home_points: int
    teams_home_total_points: int
    teams_home_standings: int
    teams_home_last_five_matches_points: int
    teams_away_total_goals_scored: int
    teams_away_total_goals_lost: int
    teams_away_points: int
    teams_away_total_points: int
    teams_away_standings: int
    teams_away_last_five_matches_points: int

class FixtureUpdated(FixtureUpdatedBase):
    id: int

    class Config:
        orm_mode: True