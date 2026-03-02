import time
from ingest import ingest_prices
from logging_utils import write_log
from storage import insert_record

def run_pipeline():
    print("Starting CryptoPulsePipeline... (Ctrl+C to stop)")

    while True:
        record = ingest_prices()
        write_log(record)
        insert_record(record)

        print("Log + DB entry written. Waiting 30 seconds...\n")
        time.sleep(30)

if __name__ == "__main__":
    run_pipeline()