import time
import random
import typing
import concurrent.futures
import json
import threading
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime, timezone
from config import PROJECT_ID, DATASET, PRECISION
import os
# Configuration
NUM_TABLES = 100
TABLE_PREFIX = "events"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "F:\\BQ-DF\\akesh-test-483106-5b2d0bff5083.json"

# Global event for clean shutdown
STOP_EVENT = threading.Event()

client = bigquery.Client(project=PROJECT_ID)

def create_tables_if_not_exists():
    """Checks if dataset and tables exist, creates them if not."""
    dataset_ref = client.dataset(DATASET)
    
    # Check/Create Dataset
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        print(f"Dataset {DATASET} not found. Creating...")
        ds = bigquery.Dataset(dataset_ref)
        ds.location = "us-central1"
        client.create_dataset(ds)

    # Define Schema matching generate_row
    schema = [
        bigquery.SchemaField("event_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("change_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payload", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("order_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("change_timestamp", "TIMESTAMP", mode="NULLABLE"),
    ]

    print(f"Checking {NUM_TABLES} tables with prefix '{TABLE_PREFIX}'...")
    for i in range(1, NUM_TABLES + 1):
        table_name = f"{TABLE_PREFIX}_{i}"
        table_ref = dataset_ref.table(table_name)
        
        try:
            client.get_table(table_ref)
        except NotFound:
            print(f"Table {table_name} not found. Creating...")
            table = bigquery.Table(table_ref, schema=schema)
            # Partitioning helps with performance and is best practice
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="change_timestamp"
            )
            client.create_table(table)
            print(f"Created {table_name}")

def generate_row(counter_id: int) -> typing.Tuple[typing.Dict[str, typing.Any], bool]:
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    
    op_type = random.choices(['INSERT', 'UPDATE', 'DELETE'], weights=[100, 0, 0])[0]
    
    if op_type == 'INSERT' or counter_id == 1:
        target_id = str(counter_id)
        should_increment = True
    else:
        target_id = str(random.randint(1, counter_id - 1))
        should_increment = False

    payload = {
        "order_id": f"o-{now_iso}",
        "user_id": f"u-{int(time.time()) % 100}",
        "price": round(100 * time.time() % 100, 2),
        "created_at": now_iso,
        "updated_at": now_iso
    }

    row = {
        "event_id": target_id,
        "change_type": op_type,
        "payload": payload,
        "change_timestamp": now_iso
    }

    return row, should_increment

def worker_task(table_index: int, sleep_interval: float):
    table_name = f"{TABLE_PREFIX}_{table_index}"
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    log_folder = "add_data"
    os.makedirs(log_folder, exist_ok=True)
    log_filename = os.path.join(log_folder, f"{table_name}.log")

    current_counter = 1
    # Randomize start to avoid thundering herd on logs
    time.sleep(random.random() * 2)
    
    print(f"[{table_name}] Worker started sending data to {table_id}. Log: {log_filename}")
    
    while not STOP_EVENT.is_set():
        row, incremented = generate_row(current_counter)
        
        try:
            errors = client.insert_rows_json(table_id, [row])
            if errors:
                print(f"[{table_name}] Error: {errors}")
            else:
                # Less verbose logging
                if current_counter % 100 == 0:
                    print(f"[{table_name}] Inserted event_id={row['event_id']}")
                
                # Log to local file
                with open(log_filename, "a", encoding="utf-8") as f:
                    f.write(json.dumps(row) + "\n")
        except Exception as e:
             # Handle transient errors to prevent thread death
             print(f"[{table_name}] Exception: {e}")

        if incremented:
            current_counter += 1
            
        # Sleep fixed amount, but return immediately if STOP_EVENT is set
        if STOP_EVENT.wait(timeout=sleep_interval):
            break

if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Run multi-table data generator')
    parser.add_argument('--tables', type=int, default=1, required=True, help='Number of Tables')
    args = parser.parse_args()
    NUM_TABLES = args.tables
    create_tables_if_not_exists()
    print(f"Starting multi-threaded generator for {NUM_TABLES} tables...")
    
    TABLE_SLEEP_TIMES = {i: 10 for i in range(1, NUM_TABLES + 1)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_TABLES) as executor:
        futures = []
        for i in range(NUM_TABLES):
            idx = i + 1
            interval = TABLE_SLEEP_TIMES.get(idx, 2.0)
            futures.append(executor.submit(worker_task, idx, interval))
        
        # Keep main thread alive
        try:
            for future in concurrent.futures.as_completed(futures):
                future.result()
        except KeyboardInterrupt:
            print("\nReceived stop signal. Shutting down worker threads...")
            STOP_EVENT.set()
            executor.shutdown(wait=True)
            print("Done.")
            STOP_EVENT.set()