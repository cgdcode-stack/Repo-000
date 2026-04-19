# %%
import requests
import time
from datetime import datetime, timezone
from supabase import create_client

# %%
url = "https://api.open-meteo.com/v1/forecast?latitude=42.36&longitude=-71.06&current_weather=true"

# %%
# from supabase import create_client

SUPABASE_URL = "https://cohxyhjfqhbnvtsjcxev.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNvaHh5aGpmcWhibnZ0c2pjeGV2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY1NTEwODYsImV4cCI6MjA5MjEyNzA4Nn0.g5rSKFCjCL7lw52jDle09cmzEBLMeJf-RFurksf2QN0"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# %%
for i in range(5):

    # 1. FETCH API DATA
    res = requests.get(url, timeout=5)
    data = res.json()
    weather = data["current_weather"]

    # 2. TIME MAPPING (IMPORTANT PART)
    api_time = weather["time"]  # real-world timestamp
    fetched_time = datetime.now(timezone.utc).isoformat()  # ingestion time

    # 3. BUILD ROW (matches Supabase table exactly)
    row = {
        "timestamp": api_time,
        "fetched_at": fetched_time,
        "temperature_c": weather["temperature"],
        "windspeed_kmh": weather["windspeed"],
        "weather_code": weather["weathercode"],
        "location": "boston_ma"
    }

    # 4. DEBUG OUTPUT
    print(row)

    # 5. INSERT INTO SUPABASE
    supabase.table("weather_snapshots").insert(row).execute()

    # 6. WAIT (controls data frequency)
    time.sleep(10)

# %%
data = supabase.table("weather_snapshots").select("*").limit(10).execute()
print(data.data)


