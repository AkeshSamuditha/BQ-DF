import apache_beam as beam
from apache_beam.transforms.core import DoFn, RestrictionProvider, WatermarkEstimatorProvider
from apache_beam.io.watermark_estimators import WatermarkEstimator
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.utils.timestamp import Timestamp, Duration
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
import time
import logging
import typing
import json
import sys
PRECISION = 1_000_000  # For timestamp precision adjustments

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "akesh-test-483106"
BUCKET = "test-poc-no1"
REGION = "us-central1"
DATASET = "poc_cdc"
TABLE = "events"
CDC_MODE = 'polling'
POLL_INTERVAL = 30


class BQRecord(typing.NamedTuple):
    change_type: str
    payload: typing.Dict[str, typing.Any]
    change_timestamp: str

class BQCDCRestrictionTracker(OffsetRestrictionTracker):
    """Restriction tracker for BigQuery CDC SDF"""
    def try_split(self, fraction_of_remainder):
        # We delegate the complex splitting logic to OffsetRestrictionTracker.
        # It correctly handles checkpointing (fraction=0) and backlog splitting.
        split_result = super().try_split(fraction_of_remainder)
        if split_result:
            logger.info(f"Tracker split at {split_result[0].start}. Fraction: {fraction_of_remainder}")
        return split_result

    def is_bounded(self):
        # Return False to indicate this is a streaming/unbounded source
        return False

class BQCDCRestrictionProvider(RestrictionProvider):    
    def initial_restriction(self, element: typing.Any) -> OffsetRange:
        """Initialize the restriction"""
        start_dt = datetime.now(timezone.utc)
        start_micros = int(start_dt.timestamp() * PRECISION)

        # Set end time to max value for unbounded
        end_micros = sys.maxsize

        return OffsetRange(start=start_micros, stop=end_micros)


    def create_tracker(self, restriction: OffsetRange) -> BQCDCRestrictionTracker:
        """Create restriction tracker. OffsetRestrictionTracker provided by Beam Python SDK is used"""
        return BQCDCRestrictionTracker(restriction)

    def restriction_size(self, element: typing.Any, restriction: OffsetRange) -> int:
        """Calculate size of restriction"""
        return restriction.size()

    def split(self, element: typing.Any, restriction: OffsetRange) -> typing.Iterable[OffsetRange]:
        """Split restriction - no splitting for streaming"""
        return [restriction]

    # def restriction_coder(self):
    #     """Return coder for the restriction (OffsetRange)"""
    #     # Using PickleCoder to avoid import issues with specific OffsetRangeCoder
    #     from apache_beam.coders import coders
    #     return coders.PickleCoder()

class BQCDCWatermarkEstimatorProvider(WatermarkEstimatorProvider):
    def initial_estimator_state(self, element: typing.Any, restriction: OffsetRange) -> Timestamp:
        # Initialize state with 5 minutes ago. Same as restriction start.
        start_dt = datetime.now(timezone.utc) - timedelta(minutes=5)

        return Timestamp.of(start_dt.timestamp())

    def create_watermark_estimator(self, estimator_state: Timestamp) -> 'BQCDCWatermarkEstimator':
        return BQCDCWatermarkEstimator(estimator_state)

    def estimator_state_coder(self) -> typing.Any:
        from apache_beam.coders import coders
        return coders.PickleCoder()

class BQCDCWatermarkEstimator(WatermarkEstimator):
    """Simple manual watermark estimator"""
    
    def __init__(self, timestamp: typing.Optional[Timestamp]):
        self.state = timestamp
    
    def observe_timestamp(self, timestamp: Timestamp) -> None:
        # Will be invoked on each output from the SDF
        if self.state is None or timestamp > self.state:
            self.state = timestamp

    def current_watermark(self) -> typing.Optional[Timestamp]:
        """Return the current watermark"""
        return self.state

    def get_estimator_state(self) -> typing.Optional[Timestamp]:
        """Get state for checkpointing"""
        return self.state

class BQCDCDoFn(DoFn):
    """Splittable DoFn for streaming CDC from BigQuery"""

    def __init__(self, project: str, dataset: str, table: str, poll_interval: int = 30, mode: str = 'polling'):
        self.project: str = project
        self.dataset: str = dataset
        self.table: str = table
        self.POLL_INTERVAL: int = poll_interval
        self.mode: str = mode.lower()
        self.client: typing.Optional[bigquery.Client] = None

    def setup(self) -> None:
        """Initialize BigQuery client"""
        self.client = bigquery.Client(project=self.project)
        logger.info(f"BigQuery CDC initialized: {self.mode} mode for {self.project}.{self.dataset}.{self.table}")

    def _build_query(self, start_ts: str, end_ts: str) -> str:
        """Build the SQL query based on mode"""
        if self.mode == 'polling':
            return f"""
            SELECT event_id, change_type, payload, change_timestamp
            FROM `{self.project}.{self.dataset}.{self.table}`
            WHERE change_timestamp >= TIMESTAMP('{start_ts}')
              AND change_timestamp < TIMESTAMP('{end_ts}')
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
                    TIMESTAMP('{start_ts}'),
                    TIMESTAMP('{end_ts}')
                )
                ORDER BY _CHANGE_TIMESTAMP
            """

    @DoFn.unbounded_per_element()
    def process(
        self,
        element: typing.Any,
        tracker: OffsetRestrictionTracker = beam.DoFn.RestrictionParam(BQCDCRestrictionProvider()),
        watermark_estimator: BQCDCWatermarkEstimator = DoFn.WatermarkEstimatorParam(BQCDCWatermarkEstimatorProvider())
    ) -> typing.Iterable[beam.window.TimestampedValue]:
        
        # 1. Get where we are starting from
        current_start = tracker.current_restriction().start
        
        # 2. Define where we want to go (NOW)
        end_dt = datetime.now(timezone.utc)
        target_pos = int(end_dt.timestamp() * PRECISION)

        # 3. SIMPLIFICATION: Attempt to claim the whole batch UP FRONT.
        # We subtract 1 because try_claim is inclusive. 
        # By claiming (target_pos - 1), we reserve the range [current_start, target_pos).
        if not tracker.try_claim(target_pos - 1):
            return

        # 4. If we passed step 3, we OWN this time window. Just do the work.
        start_ts = datetime.fromtimestamp(current_start / PRECISION, tz=timezone.utc).isoformat()
        end_ts = end_dt.isoformat()
        
        query = self._build_query(start_ts, end_ts)

        try:
            rows = self.client.query(query).result()
            logger.info(f"[{self.mode}] Processing window {start_ts} to {end_ts}. count={rows.total_rows}")
            
            for row in rows:
                event_dt: typing.Optional[datetime] = getattr(row, "change_timestamp", None)
                
                if event_dt:
                    event_ts_seconds = event_dt.timestamp()
                    row_dict = dict(row)
                    row_dict["change_timestamp"] = event_dt.isoformat()
                    # ... other transformations ...

                    # Observe watermark strictly per element
                    watermark_estimator.observe_timestamp(Timestamp.of(event_ts_seconds))
                    yield beam.window.TimestampedValue(row_dict, event_ts_seconds)

            # 5. Handle "Empty Window" Watermark updates
            if rows.total_rows == 0:
                watermark_estimator.observe_timestamp(Timestamp.of(end_dt.timestamp()))

        except Exception as e:
            logger.error(f"Failed to poll {start_ts} to {end_ts}: {e}")
            # Raise ensures the runner retries this specific bundle
            raise e

        # 6. Defer the FUTURE.
        # Since we claimed up to 'target_pos', the runner knows next time starts at 'target_pos'
        tracker.defer_remainder(Duration.of(seconds=self.POLL_INTERVAL))

def run() -> None:
    options = beam.options.pipeline_options.PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        temp_location=f"gs://{BUCKET}/{DATASET}/{TABLE}/temp",
        staging_location=f"gs://{BUCKET}/{DATASET}/{TABLE}/staging",
        streaming=True,
        save_main_session=True,
        num_workers=1,
        max_num_workers=2,
        # worker_machine_type="n1-standard-1"
    )

    logger.info("Building pipeline...")

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Start" >> beam.Impulse()
            | "CDC Source" >> beam.ParDo(BQCDCDoFn(
                PROJECT_ID, 
                DATASET, 
                TABLE, 
                poll_interval=POLL_INTERVAL,
                mode=CDC_MODE
            ))
            # | "Deduplicate" >> beam.Distinct(lambda x: f"{x['change_type']}-{x['payload']['order_id'] or x['event_id']}-{x['change_timestamp']}")
            | "Format JSON" >> beam.Map(lambda x: json.dumps(x, default=str))
            | "Window" >> beam.WindowInto(beam.window.FixedWindows(60))
            | "Write" >> beam.io.WriteToText(
                f"gs://{BUCKET}/{DATASET}/{TABLE}/output",
                file_name_suffix=".json"
            )
        )

    logger.info("Pipeline submitted to Dataflow!")


if __name__ == '__main__':
    run()
