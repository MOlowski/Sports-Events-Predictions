from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Bet(Base):
    __tablename__ = 'bets'
    id = Column(Integer, primary_key=True, index=True)
    fixture_id = Column(Integer, ForeignKey('fixtures.id'))
    bet_probability = Column(Float)
    bet_value = Column(Float)
    bet_name = Column(String)
    bet_number = Column(Integer)
    fixture = relationship('Fixture')

class Fixture():
    __tablename__ = 'fixtures'
    id = Column(Integer, primary_key=True, index=True)
    home_team_id = Column(Integer)
    away_team_id = Column(Integer)
    league_id = Column(Integer)
    date = Column(String)
    league = relationship('League')

class Team(Base):
    __tablename__ = 'teams'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)

class League(Base):
    __tablename__ = 'leagues'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)