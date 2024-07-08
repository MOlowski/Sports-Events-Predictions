from pydantic import BaseModel

class BetBase(BaseModel):
    fixture_id: int
    bet_probability: float
    bet_value: float
    bet_name: str
    bet_number: int


class Bet(BetBase):
    id: int

    class Config:
        orm_mode = True    
    
# Define schemas for Fixture, Team, and League similarly