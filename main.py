import requests
import json
from datetime import datetime, timedelta
from time import sleep

# Define the base URL
base_url = "http://localhost:8181/shift"

# Define an array of shifts
shifts = [
    {
        "companyId": "acme-corp",
        "userId": "user123",
        "startTime": "2025-06-15T08:00:00",
        "endTime": "2025-06-15T16:00:00",
        "action": "add"
    },
    {
        "companyId": "tech-co",
        "userId": "user456",
        "startTime": "2025-06-16T09:00:00",
        "endTime": "2025-06-16T17:00:00",
        "action": "add"
    },
    {
        "companyId": "dev-inc",
        "userId": "user789",
        "startTime": "2025-06-17T08:00:00",
        "endTime": "2025-06-17T16:00:00",
        "action": "add"
    },
    {
        "companyId": "soft-sys",
        "userId": "user234",
        "startTime": "2025-06-18T09:00:00",
        "endTime": "2025-06-18T17:00:00",
        "action": "add"
    },
    {
        "companyId": "data-corp",
        "userId": "user567",
        "startTime": "2025-06-19T08:00:00",
        "endTime": "2025-06-19T16:00:00",
        "action": "add"
    },
    {
        "companyId": "cloud-ltd",
        "userId": "user890",
        "startTime": "2025-06-20T09:00:00",
        "endTime": "2025-06-20T17:00:00",
        "action": "add"
    }
]

# Headers for the request
headers = {
    'Content-Type': 'application/json'
}

existing_shifts = []

def update_existing_shifts():
    global existing_shifts
    try :
        # Get existing shifts first
        get_response = requests.get("http://localhost:8181/shifts")
        existing_shifts = get_response.json().get('shifts', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching existing shifts: {e}")

def shift_exists(shift):
    """Check if a shift already exists in the existing shifts."""
    for existing_shift in existing_shifts:
        if (existing_shift['companyId'] == shift['companyId'] and
            existing_shift['userId'] == shift['userId'] and
            existing_shift['startTime'] == shift['startTime'] and
            existing_shift['endTime'] == shift['endTime']):
            return True
    return False

for shift in shifts:
    update_existing_shifts()
    print(f"Existing shifts: {len(existing_shifts)}")

    print(shift.get('userId'))

    if shift_exists(shift):
        print(f"Shift for user {shift['userId']} already exists, skipping.")
        continue

    while not shift_exists(shift):
        response = requests.post(base_url, headers=headers, data=json.dumps(shift))
        sleep(0.3)  # Sleep for 300ms
        update_existing_shifts()
    

print(f"Existing shifts: {len(existing_shifts)}")
