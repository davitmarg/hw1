from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, select
from pydantic import BaseModel
from typing import List
from datetime import datetime
import asyncio
import httpx
import os
from dotenv import load_dotenv
import uvicorn

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")

DATABASE_URL = f"mysql+asyncmy://{user}:{password}@{host}:{port}/{dbname}"

engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)
Base = declarative_base()

app = FastAPI()

BASE_URL = "http://localhost:8181/shift"
GET_SHIFTS_URL = "http://localhost:8181/shifts"

class ShiftRequest(Base):
    __tablename__ = "shift_requests"
    id = Column(Integer, primary_key=True, index=True)
    status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)

class Shift(BaseModel):
    companyId: str
    userId: str
    startTime: str
    endTime: str
    action: str

async def get_existing_shifts_async():
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(GET_SHIFTS_URL)
            res.raise_for_status()
            return res.json().get('shifts', [])
    except httpx.RequestError as e:
        print(f"Error fetching existing shifts: {e}")
        return []

async def shift_exists_async(shift):
    existing_shifts = await get_existing_shifts_async()
    for ex in existing_shifts:
        if (ex['companyId'] == shift['companyId'] and
            ex['userId'] == shift['userId'] and
            ex['startTime'] == shift['startTime'] and
            ex['endTime'] == shift['endTime']):
            return True
    return False

async def add_shift_async(shift):
    headers = {'Content-Type': 'application/json'}
    try:
        if await shift_exists_async(shift):
            print(f"Shift for user {shift['userId']} already exists, skipping.")
            return True
        async with httpx.AsyncClient() as client:
            while not await shift_exists_async(shift):
                try:
                    await client.post(BASE_URL, headers=headers, json=shift)
                    await asyncio.sleep(0.3)
                except Exception as e:
                    print(f"Failed to post shift: {e}")
                    return False
        return True
    except Exception as e:
        print(f"Failed to process shift: {e}")
        return False

@app.post("/shifts")
async def post_shifts_endpoint(shifts: List[Shift], db: AsyncSession = Depends(lambda: async_session())):
    shift_request = ShiftRequest(status="pending")
    db.add(shift_request)
    await db.flush()
    await db.commit()

    asyncio.create_task(process_shift_request(shift_request.id, shifts))

    return {
        "message": "Shifts request received",
        "request_id": shift_request.id
    }

@app.get("/shifts/status/{request_id}")
async def get_shift_status(request_id: int, db: AsyncSession = Depends(lambda: async_session())):
    request = await db.get(ShiftRequest, request_id)
    return {"status": request.status if request else "not_found"}

@app.get("/shifts")
async def get_shifts_endpoint():
    shifts = await get_existing_shifts_async()
    return {"shifts": shifts}

async def process_shift_request(request_id: int, shifts: List[Shift]):
    async with async_session() as db:
        request = await db.get(ShiftRequest, request_id)
        if not request:
            return

        request.status = "processing"
        await db.commit()

        all_successful = True
        for shift in shifts:
            shift_data = shift.dict()
            success = await add_shift_async(shift_data)
            if not success:
                all_successful = False

        request.status = "done" if all_successful else "failed"
        await db.commit()

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
