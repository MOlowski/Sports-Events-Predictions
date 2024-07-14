from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import date

Base = declarative_base()

class Bet(Base):
    __tablename__ = 'bets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(Integer, ForeignKey('fixtures.fixture_id'))
    bet_prob = Column(Float)
    bet_value = Column(Float)
    bet_name = Column(String)
    bet_number = Column(Integer)
    fixture = relationship("Fixture", back_populates="bets")

class Fixture(Base):
    __tablename__ = 'fixtures'

    fixture_id = Column(Integer, primary_key=True)
    fixture_date = Column(Date, nullable=False)
    bets = relationship("Bet", back_populates="fixture")
    updated_info = relationship("FixtureUpdated", uselist=False, back_populates="fixture")

class FixtureUpdated(Base):
    __tablename__ = 'fixtures_updated'
    id = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(Integer, ForeignKey('fixtures.fixture_id'))
    some_column = Column(String)
    another_column = Column(String)
    fixture = relationship("Fixture", back_populates="updated_info")

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
    fixture = relationship("Fixture", back_populates="updated_info")