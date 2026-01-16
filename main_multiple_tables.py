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
from config import PRECISION

# Import shared classes to ensure pickle compatibility on workers
from bq_common import (
    BQTableConfig, 
    BQRecord, 
    BQCDCWatermarkEstimatorProvider, 
    BQCDCWatermarkEstimator,
    WriteTableWindowToGCS
)

from restriction import (
    BQCDCRestrictionProvider,
    BQCDCRestrictionTracker
)

from dedup import DeduplicateDoFn
from utils import (
    convert_macros_to_seconds, 
    convert_macros_to_ts,
    convert_seconds_to_macros, 
    simulate_cpu_work
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
    # Format window time as readable datetime
    window_start = window.start.to_utc_datetime().strftime('%Y%m%d-%H%M%S')
    window_end = window.end.to_utc_datetime().strftime('%Y%m%d-%H%M%S')
    return f"{destination}/output-{window_start}-to-{window_end}-{shard_index:05d}-of-{total_shards:05d}{compression}.json"

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
        start_str = convert_macros_to_ts(start_micros)
        end_str = convert_macros_to_ts(end_micros)
        logger.info(f"Building query for {table_name} from {start_str} to {end_str}")
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
    def setup(self):
        self.client = bigquery.Client(project=self.project)
        return super().setup

    @DoFn.unbounded_per_element()
    def process(
        self,
        element: BQTableConfig,
        tracker: BQCDCRestrictionTracker = beam.DoFn.RestrictionParam(BQCDCRestrictionProvider()),
        watermark_estimator: BQCDCWatermarkEstimator = DoFn.WatermarkEstimatorParam(BQCDCWatermarkEstimatorProvider())
    ) -> typing.Iterable[beam.window.TimestampedValue]:
        
        current_pos = tracker.current_restriction().start
        now = datetime.now(timezone.utc)
        target_pos = int(convert_seconds_to_macros(now.timestamp()))

        # move to next poll if caught up. In practical this should never happen 
        if current_pos >= target_pos:
            logger.info("Caught up to current time, deferring...")
            tracker.defer_remainder(Duration.of(seconds=element.poll_interval)) # cause checkpointing and call split to update restriction state for next process
            return
       
        try:
            previous_succesfull_claim = tracker.current_restriction().prev_succefull_claim
            logger.info(f"[{element.table_name}] Current Pos: {current_pos}, Target Pos: {target_pos}, Previous Successful Claim: {previous_succesfull_claim}")
            query_start_pos = (current_pos + previous_succesfull_claim) // 2 
            query = self._build_query(element.table_name, query_start_pos, target_pos)
            
            # Simulate REAL work (CPU intensive) to trigger autoscaling
            simulate_cpu_work()
            
            query_job = self.client.query(query)
            rows = list(query_job.result())
            
            if len(rows) == 0:
                logger.info(f"No new records found at {now.isoformat()} between {convert_macros_to_ts(current_pos)} and {convert_macros_to_ts(target_pos)}")
                # if no rows found do nothing
                # If no rows found for a certain time, eg: 1 hour. We can advance the watermark and restriction
                tracker.defer_remainder(Duration.of(seconds=element.poll_interval))
                return
            
            # Track the max timestamp across all processed records
            max_claimed_pos = current_pos # Initialize to current position
            logger.info(f"Processing {len(rows)} records at for table {element.table_name} from {convert_macros_to_ts(query_start_pos)} to {convert_macros_to_ts(target_pos)}")
            for row in rows:
                payload = dict(row)
                
                # ASSUMPTION - change_ts is always present and is a datetime
                ts_dt = payload.get('change_timestamp')
                ts_seconds = ts_dt.timestamp()
                record_pos = int(convert_seconds_to_macros(ts_seconds))
                
                # Track max position seen
                max_claimed_pos = max(max_claimed_pos, record_pos)
                
                record = BQRecord(
                    table_name=element.table_name,
                    event_id = payload.get('event_id', "NULL"),
                    change_type=payload.get('change_type', 'UNKNOWN'),
                    payload=payload.get('payload', {}),
                    change_timestamp=ts_dt.isoformat() if ts_dt else now.isoformat(),
                    is_late = record_pos < current_pos
                    
                )
                
                watermark_estimator.observe_timestamp(Timestamp.of(ts_seconds-30))  # small lag to avoid watermark going beyond data
                yield beam.window.TimestampedValue(record, ts_seconds)

            # Claim ONCE at the end using the max timestamp seen
            if not tracker.try_claim(max_claimed_pos):
                logger.warning(f"[{element.table_name}] Final claim failed at {max_claimed_pos} with current position {convert_macros_to_ts(current_pos)}.")
                tracker.defer_remainder(Duration.of(seconds=element.poll_interval))
                return
            logger.info(f"[{element.table_name}] Successfully claimed up to {convert_macros_to_ts(max_claimed_pos)}   :  {max_claimed_pos}.")   
        except Exception as e:
            logger.error(f"Error querying BQ: {e}")
            raise e

        tracker.defer_remainder(Duration.of(seconds=element.poll_interval))


def run(args, beam_args) -> None:
    NUM_TABLES = args.tables
    options = beam.options.pipeline_options.PipelineOptions(
        flags=beam_args,
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        temp_location=f"gs://{BUCKET}/{DATASET}/temp",
        staging_location=f"gs://{BUCKET}/{DATASET}/staging",
        streaming=True,
        save_main_session=True,
        num_workers=1,
        max_num_workers=NUM_TABLES,
        setup_file='./setup.py',
        job_name=f'bq-cdc-{args.run_id}',
        # worker_machine_type="n1-standard-1",
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
            # | "Key by EventID" >> beam.Map(lambda r: (f"{r.table_name}:{r.event_id}", r))
            # | "Deduplicate" >> beam.ParDo(DeduplicateDoFn())
            | "Window" >> beam.WindowInto(beam.window.FixedWindows(60))
            # | "KeyByTable" >> beam.Map(lambda r: (r.table_name, r))
            # | "WriteSingleFilePerTable" >> beam.ParDo(WriteTableWindowToGCS(BUCKET, DATASET))
            | "Write" >> fileio.WriteToFiles(
                path=f"gs://{BUCKET}/{DATASET}/",
                destination=lambda record: record.table_name,
                sink=JsonSink(),
                file_naming=table_naming,
                # shards=1, # 1 => 1 file per table per window. Not scalable for high throughput tables. Need to merge files downstream
                )
        )

    logger.info("Pipeline submitted to Dataflow!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run BQ CDC pipeline')
    parser.add_argument('--run_id', type=str, required=True, help='Name of the Job')
    parser.add_argument('--tables', type=int, default=2, required=True, help='Number of Tables')
    known_args, beam_args = parser.parse_known_args()
    
    run(known_args, beam_args)
