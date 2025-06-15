from fastapi import FastAPI
from typing import List
from pydantic import BaseModel
import requests
import json
from time import sleep
import uvicorn

app = FastAPI()

BASE_URL = "http://localhost:8181/shift"
GET_SHIFTS_URL = "http://localhost:8181/shifts"

class Shift(BaseModel):
    companyId: str
    userId: str
    startTime: str
    endTime: str
    action: str

def get_existing_shifts():
    try:
        res = requests.get(GET_SHIFTS_URL)
        res.raise_for_status()
        return res.json().get('shifts', [])
    except requests.RequestException as e:
        print(f"Error fetching existing shifts: {e}")
        return []

def shift_exists(shift):
    existing_shifts = get_existing_shifts()
    for ex in existing_shifts:
        if (ex['companyId'] == shift['companyId'] and
            ex['userId'] == shift['userId'] and
            ex['startTime'] == shift['startTime'] and
            ex['endTime'] == shift['endTime']):
            return True
    return False

def add_shift(shift):
    headers = {'Content-Type': 'application/json'}
    if shift_exists(shift):
        print(f"Shift for user {shift['userId']} already exists, skipping.")
        return
    while not shift_exists(shift):
        response = requests.post(BASE_URL, headers=headers, data=json.dumps(shift))
        sleep(0.3)

def post_shifts(shifts):
    for shift in shifts:
        add_shift(shift)
    return get_existing_shifts()

@app.post("/shifts")
async def post_shifts_endpoint(shifts: List[Shift]):
    shifts_dicts = [shift.dict() for shift in shifts]
    updated_shifts = post_shifts(shifts_dicts)
    return {"message": "Shifts processed", "shifts": updated_shifts}

@app.get("/shifts")
def get_shifts_endpoint():
    shifts = get_existing_shifts()
    return {"shifts": shifts}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
