import requests
import time
import uuid
import json

API_KEY = "dkuaps-IzJWpKI1UvAKiyWZ9sJzHXSIMNVVVMt7"
BASE_URL = "http://localhost:28315"
PROJECT_KEY = "COMPARES3"
SCENARIO_ID = "COMPARE_S3"

request_id = uuid.uuid4()

params = json.dumps({ "requestId": request_id }, default=str)
print(params)

# Step 1: Trigger the scenario
trigger_url = f"{BASE_URL}/public/api/projects/{PROJECT_KEY}/scenarios/{SCENARIO_ID}/run"
resp = requests.post(trigger_url, auth=(API_KEY, ""), json=json.loads(params))
resp.raise_for_status()

# there seem to be a bug with the runId retured, therefore we use our own requestId to match the job below
run_id = resp.json()["runId"]
print(f"Triggered run: {request_id}")

wait = True

# Poll status
status_url = f"{BASE_URL}/public/api/projects/{PROJECT_KEY}/scenarios/{SCENARIO_ID}/get-last-runs?limit=5"
while wait:
    r = requests.get(status_url, auth=(API_KEY, ""))
    if r.status_code == 404:
        print("Run not found yet…")
    else:
        r.raise_for_status()
        
        for data in r.json():
            print (data["trigger"]["params"])            
            if "requestId" in data["trigger"]["params"]:
                if str(data["trigger"]["params"]["requestId"]) == str(request_id):

                    if "result" in data and data["result"]:
                        result = data["result"]
                        print(f"Current status: {result['outcome']}")
                        if result["outcome"] in ["SUCCESS", "FAILED"]:
                            wait = False
                            break
    time.sleep(5)
