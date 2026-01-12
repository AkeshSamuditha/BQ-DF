import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.utils.timestamp import Timestamp, Duration
from apache_beam.io import fileio
from google.cloud import bigquery
from datetime import datetime, timezone
import logging
import typing
import json
import argparse

# Import shared classes to ensure pickle compatibility on workers
from bq_common import (
    BQTableConfig, 
    BQRecord, 
    BQCDCRestrictionProvider, 
    BQCDCWatermarkEstimatorProvider, 
    BQCDCWatermarkEstimator,
    PRECISION
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom Sink for JSON Output
class JsonSink(fileio.FileSink):
    def open(self, fh):
        self.fh = fh
    def write(self, record):
        self.fh.write(json.dumps(record._asdict(), default=str).encode('utf-8'))
        self.fh.write(b'\n')
    def flush(self):
        self.fh.flush()

# File naming function for directory-per-table structure
def table_naming(window, pane, shard_index, total_shards, compression, destination):
    # Destination is the table_name
    return f"{destination}/output-{window}-{shard_index:05d}-of-{total_shards:05d}{compression}.json"

# Configuration
PROJECT_ID = "akesh-test-483106"
BUCKET = "test-poc-no1"
REGION = "us-central1"
DATASET = "poc_cdc"

# DoFn definition
class BQCDCDoFn(DoFn):
    """Splittable DoFn for streaming CDC from BigQuery"""

    def __init__(self, project: str, dataset: str):
        self.project: str = project
        self.dataset: str = dataset
        self.mode: str = 'polling'  # or 'change_stream'

    def _build_query(self, table_name, start_micros, end_micros):
        """Construct the SQL query based on mode"""
        start_ts_sec = start_micros / PRECISION
        end_ts_sec = end_micros / PRECISION
        
        start_dt = datetime.fromtimestamp(start_ts_sec, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ts_sec, tz=timezone.utc)
        
        start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        if self.mode == 'polling':
            return f"""
            SELECT event_id, change_type, payload, change_timestamp
            FROM `{self.project}.{self.dataset}.{table_name}`
            WHERE change_timestamp >= TIMESTAMP('{start_str}')
              AND change_timestamp < TIMESTAMP('{end_str}')
            ORDER BY change_timestamp ASC
            """
        else:
            return f"""
            SELECT
                _CHANGE_TYPE AS change_type,
                _CHANGE_TIMESTAMP AS change_timestamp,
                STRUCT(* EXCEPT(_CHANGE_TYPE, _CHANGE_TIMESTAMP)) AS payload
                FROM CHANGES(
                    `project.dataset.events`,
                    TIMESTAMP('{start_str}'),
                    TIMESTAMP('{end_str}')
                )
                ORDER BY _CHANGE_TIMESTAMP
            """

    @DoFn.unbounded_per_element()
    def process(
        self,
        element: BQTableConfig,
        tracker: OffsetRestrictionTracker = beam.DoFn.RestrictionParam(BQCDCRestrictionProvider()),
        watermark_estimator: BQCDCWatermarkEstimator = DoFn.WatermarkEstimatorParam(BQCDCWatermarkEstimatorProvider())
    ) -> typing.Iterable[beam.window.TimestampedValue]:
        
        current_pos = tracker.current_restriction().start
        now = datetime.now(timezone.utc)
        target_pos = int(now.timestamp() * PRECISION)

        # 
        if current_pos >= target_pos:
            tracker.defer_remainder(Duration.of(seconds=element.poll_interval))
            return

        if not tracker.try_claim(target_pos - 1):
             logger.info("Claim failed. Stopping.")
             return

        try:
            query = self._build_query(element.table_name, current_pos, target_pos)
            
            # Simulate REAL work (CPU intensive) to trigger autoscaling
            self._simulate_cpu_work()
            
            client = bigquery.Client(project=self.project)
            query_job = client.query(query)
            rows = list(query_job.result())
            
            if len(rows) == 0:
                # if no rows found advance watermark to target
                watermark_estimator.observe_timestamp(Timestamp.of(target_pos / PRECISION))
            for row in rows:
                payload = dict(row)
                
                # ASSUMPTION - change_ts is always present and is a datetime
                ts_dt = payload.get('change_timestamp')

                ts_seconds = int(ts_dt.timestamp())
                
                record = BQRecord(
                    table_name=element.table_name,
                    event_id = payload.get('event_id', "NULL"),
                    change_type=payload.get('change_type', 'UNKNOWN'),
                    payload=payload.get('payload', {}),
                    change_timestamp=ts_dt.isoformat() if ts_dt else now.isoformat()
                )
                
                watermark_estimator.observe_timestamp(Timestamp.of(ts_seconds))
                yield beam.window.TimestampedValue(record, ts_seconds)

        except Exception as e:
            logger.error(f"Error querying BQ: {e}")
            raise e

        tracker.defer_remainder(Duration.of(seconds=element.poll_interval))

    def _simulate_cpu_work(self):
        """Burn CPU to simulate heavy processing logic (e.g. parsing, enrichment)."""
        # Calculate primes to burn CPU cycles
        count = 0
        num = 2
        while count < 3000: 
            is_prime = True
            for i in range(2, int(num ** 0.5) + 1):
                if num % i == 0:
                    is_prime = False
                    break
            if is_prime:
                count += 1
            num += 1

def run(args) -> None:
    NUM_TABLES = 100  # Adjust as needed
    options = beam.options.pipeline_options.PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        temp_location=f"gs://{BUCKET}/{DATASET}/temp",
        staging_location=f"gs://{BUCKET}/{DATASET}/staging",
        streaming=True,
        save_main_session=True,
        num_workers=1,
        max_num_workers=NUM_TABLES,
        setup_file='./setup.py', # Add setup.py to package common module
        job_name=f'bq-cdc-{args.run_id}'
        # worker_machine_type="n1-standard-1"
    )

    logger.info("Building pipeline...")

    # Define tables configuration
    table_configs = [
        BQTableConfig(table_name=f"events_{i}", poll_interval=30)
        for i in range(1, NUM_TABLES + 1)
    ]

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "CreateConfigs" >> beam.Create(table_configs)
            | "CDC Source" >> beam.ParDo(BQCDCDoFn(PROJECT_ID, DATASET))
            | "Window" >> beam.WindowInto(beam.window.FixedWindows(60))
            | "Write" >> fileio.WriteToFiles(
                path=f"gs://{BUCKET}/{DATASET}/",
                destination=lambda record: record.table_name,
                sink=JsonSink(),
                file_naming=table_naming
            )
        )

    logger.info("Pipeline submitted to Dataflow!")


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Run BQ CDC pipeline')
    parser.add_argument('--run_id', type=str, required=True, help='Name of the Job')
    parser.add_argument('--tables', type=int, default=1, required=True, help='Number of Tables')
    args = parser.parse_args()
    
    run(args)
