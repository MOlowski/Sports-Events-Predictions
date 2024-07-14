from fastapi import FastAPI
from .database import engine, Base
from .api import bets

app = FastAPI()

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

app.include_router(bets.router, prefix="/bets")

@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}
