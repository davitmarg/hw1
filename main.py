import uuid
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, select, func
from pydantic import BaseModel
from typing import List
from datetime import datetime
import boto3
import json

import asyncio
import httpx
import os
from dotenv import load_dotenv
import uvicorn
from contextlib import asynccontextmanager

load_dotenv()

sqs = boto3.client(
    "sqs",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)
SQS_URL = os.getenv("SQS_URL")


user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")
shard_count = int(os.getenv("DB_SHARD_COUNT", 1))

BASE_DATABASE_URL = f"mysql+asyncmy://{user}:{password}@{host}:{port}/{dbname}"

def send_to_sqs(payload: str):
    sqs = boto3.client(
        'sqs',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    sqs.send_message(
        QueueUrl=os.getenv("SQS_URL"),
        MessageBody=payload
    )

for i in range(shard_count):
    print(f"Shard {i}: {BASE_DATABASE_URL}_{i}")

shard_engines = [
    create_async_engine(f"{BASE_DATABASE_URL}_{i}", echo=True)
    for i in range(shard_count)
]
shard_sessions = [
    async_sessionmaker(engine, expire_on_commit=False)
    for engine in shard_engines
]


def get_shard_index_by_user_id(user_id: str) -> int:
    return hash(user_id) % shard_count

def get_shard_session_by_user_id(user_id: str) -> AsyncSession:
    shard_idx = get_shard_index_by_user_id(user_id)
    return shard_sessions[shard_idx]()

Base = declarative_base()

@asynccontextmanager
async def lifespan(app: FastAPI):
    for engine in shard_engines:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    yield

app = FastAPI(lifespan=lifespan)

BASE_URL = "http://localhost:8181/shift"
GET_SHIFTS_URL = "http://localhost:8181/shifts"

class StoredShift(Base):
    __tablename__ = "stored_shifts"
    id = Column(Integer, primary_key=True, index=True) 
    request_id = Column(String(36), nullable=False)
    status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)

    companyId = Column(String(255))
    userId = Column(String(255))
    startTime = Column(String(255))
    endTime = Column(String(255))
    action = Column(String(255))

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
async def post_shifts_endpoint(shifts: List[Shift]):
    request_id = str(uuid.uuid4())

    for shift in shifts:
        shard_id = get_shard_index_by_user_id(shift.userId)
        async with shard_sessions[shard_id]() as db:
            stored_shift = StoredShift(
                request_id=request_id,
                companyId=shift.companyId,
                userId=shift.userId,
                startTime=shift.startTime,
                endTime=shift.endTime,
                action=shift.action
            )
            db.add(stored_shift)
            await db.commit()

    asyncio.create_task(process_shifts_background(request_id))
    return {"message": "Shifts stored", "request_id": request_id}

@app.get("/shifts/status/{request_id}")
async def get_shift_status(request_id: str):
    statuses = []
    for idx in range(shard_count):
        async with shard_sessions[idx]() as db:
            result = await db.execute(select(StoredShift).where(
                StoredShift.request_id == request_id
            ))
            shifts = result.scalars().all()
            statuses += [s.status for s in shifts]

    if not statuses:
        return {"status": "not_processing"}
    if all(s == "done" for s in statuses):
        return {"status": "done"}
    if all(s == "failed" for s in statuses):
        return {"status": "failed"}
    if any(s in ["pending", "processing"] for s in statuses):
        return {"status": "processing"}
    return {"status": "partial_failure"}

@app.get("/shifts")
async def get_shifts_endpoint():
    shifts = await get_existing_shifts_async()
    return {"shifts": shifts}

async def process_shifts_background(request_id: int):

    for idx in range(shard_count):
        async with shard_sessions[idx]() as db:
            result = await db.execute(select(StoredShift).where(
                StoredShift.request_id == request_id,
                StoredShift.status == "pending"
            ))
            shifts = result.scalars().all()

            for stored_shift in shifts:
                stored_shift.status = "processing"
                await db.commit()

                shift_data = {
                    "companyId": stored_shift.companyId,
                    "userId": stored_shift.userId,
                    "startTime": stored_shift.startTime,
                    "endTime": stored_shift.endTime,
                    "action": stored_shift.action
                }

                success = await add_shift_async(shift_data)
                stored_shift.status = "done" if success else "failed"

                if success:
                    send_to_sqs(json.dumps({
                        "id": stored_shift.id,
                        "request_id": stored_shift.request_id,
                        "status": stored_shift.status,
                        "created_at": stored_shift.created_at.isoformat() if stored_shift.created_at else None,
                        "companyId": stored_shift.companyId,
                        "userId": stored_shift.userId,
                        "startTime": stored_shift.startTime,
                        "endTime": stored_shift.endTime,
                        "action": stored_shift.action
                    }))

                await db.commit()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
