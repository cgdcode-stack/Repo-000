import logging
import time
from datetime import datetime, timezone

import requests

from weather_app.client import get_supabase_client


URL = "https://api.open-meteo.com/v1/forecast?latitude=42.36&longitude=-71.06&current_weather=true"

logger = logging.getLogger(__name__)


def fetch_weather(max_retries=3, retry_delay=2):
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            res = requests.get(URL, timeout=10)
            res.raise_for_status()
            data = res.json()
            return data["current_weather"]
        except Exception as e:
            last_error = e
            logger.warning("Fetch attempt %s failed: %s", attempt, e)
            if attempt < max_retries:
                time.sleep(retry_delay)

    raise last_error


def run_pipeline():
    supabase = get_supabase_client()

    logger.info("Starting weather pipeline run")

    weather = fetch_weather()

    api_time = weather["time"]
    fetched_time = datetime.now(timezone.utc).isoformat()

    row = {
        "timestamp": api_time,
        "fetched_at": fetched_time,
        "temperature_c": weather["temperature"],
        "windspeed_kmh": weather["windspeed"],
        "weather_code": weather["weathercode"],
        "location": "boston_ma",
    }

    logger.info("Prepared row: %s", row)

    result = (
        supabase.table("weather_snapshots")
        .upsert(row, on_conflict="timestamp,location")
        .execute()
    )

    logger.info("Insert completed successfully")
    logger.info("Insert result: %s", result.data)

    recent_data = (
        supabase.table("weather_snapshots")
        .select("*")
        .order("fetched_at", desc=True)
        .limit(10)
        .execute()
    )

    logger.info("Most recent rows: %s", recent_data.data)