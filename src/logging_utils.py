import json
from datetime import datetime
import os

def write_log(record):
    # Ensure logs/ folder exists
    os.makedirs("logs", exist_ok=True)

    # Build today's log filename
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"logs/pipeline_{today}.jsonl"

    # Write one JSON line
    with open(filename, "a") as f:
        f.write(json.dumps(record) + "\n")