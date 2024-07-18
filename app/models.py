from sqlalchemy import Column, Integer, String, Boolean, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import date

Base = declarative_base()

class Bet(Base):
    __tablename__ = 'bets'
    fixture_id = Column(Integer, ForeignKey('fixtures.fixture_id'))
    bet_prob = Column(Float)
    bet_value = Column(Float)
    bet_name = Column(String)
    bet_number = Column(Integer)
    id = Column(Integer, primary_key=True, autoincrement=True)
    fixture = relationship("Fixture", back_populates="bets")
    bets_results = relationship(
        "BetResult",
        primaryjoin="and_(Bet.fixture_id == foreign(BetResult.fixture_id), Bet.bet_name == foreign(BetResult.bet_name))",
        back_populates="bet"
    )
class Fixture(Base):
    __tablename__ = 'fixtures'

    fixture_id = Column(Integer, primary_key=True)
    fixture_date = Column(Date, nullable=False)
    league_id = Column(Integer, ForeignKey('leagues.league_id'))
    teams_home_id = Column(Integer, ForeignKey('teams.team_id'))
    bets = relationship("Bet", back_populates="fixture")
    updated_info = relationship("FixtureUpdated", uselist=False, back_populates="fixture")
    bets_res = relationship("BetResult", back_populates="fix")
    team_home = relationship("Team", foreign_keys=[teams_home_id], back_populates='fixtures_home')
    league = relationship("League", back_populates="fixture_l")

class FixtureUpdated(Base):
    __tablename__ = 'fixtures_updated'
    id = Column(Integer, primary_key=True, autoincrement=True)
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
    fixture = relationship("Fixture", back_populates="updated_info")

class BetResult(Base):
    __tablename__ = 'bets_results'
    fixture_id = Column(Integer, ForeignKey('fixtures.fixture_id'))
    bet_name = Column(String, primary_key=True)
    bet_number = Column(Integer)
    match_result = Column(Boolean)
    bet_result = Column(Boolean)
    bet = relationship(
        "Bet",
        primaryjoin="and_(Bet.fixture_id == foreign(BetResult.fixture_id), Bet.bet_name == foreign(BetResult.bet_name))",
        back_populates="bets_results"
    )
    fix = relationship("Fixture", back_populates="bets_res")

class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String)
    fixtures_home = relationship("Fixture", foreign_keys=[Fixture.teams_home_id], back_populates="team_home")

class League(Base):
    __tablename__ = 'leagues'
    league_id = Column(Integer, primary_key=True)
    name = Column(String)
    country = Column(String)
    fixture_l = relationship("Fixture", back_populates="league")