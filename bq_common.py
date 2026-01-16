import typing
import logging
import sys
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.transforms.core import RestrictionProvider, WatermarkEstimatorProvider
from apache_beam.io.watermark_estimators import WatermarkEstimator
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.utils.timestamp import Timestamp
from config import PRECISION
import json
from apache_beam.io import filesystems
from apache_beam.transforms.core import DoFn

logger = logging.getLogger(__name__)

# Class to hold configuration for each table
class BQTableConfig(typing.NamedTuple):
    table_name: str
    poll_interval: int = 30

class BQRecord(typing.NamedTuple):
    event_id: str
    change_type: str
    change_timestamp: str
    table_name: str
    payload: typing.Dict[str, typing.Any]
    is_late: bool = False
    is_duplicate: typing.Optional[bool] = False

class BQCDCRestrictionTracker(OffsetRestrictionTracker):
    """Restriction tracker for BigQuery CDC SDF"""
    def try_split(self, fraction_of_remainder):
        # We delegate the complex splitting logic to OffsetRestrictionTracker.
        split_result = super().try_split(fraction_of_remainder)
        if split_result:
            logger.info(f"Tracker split at {split_result[0].start}. Fraction: {fraction_of_remainder}")
        return split_result

    def is_bounded(self):
        # Return False to indicate this is a streaming/unbounded source
        return False

class BQCDCRestrictionProvider(RestrictionProvider):    
    def initial_restriction(self, element: BQTableConfig) -> OffsetRange:
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

class BQCDCWatermarkEstimatorProvider(WatermarkEstimatorProvider):
    def initial_estimator_state(self, element: typing.Any, restriction: OffsetRange) -> Timestamp:
        # Initialize state. Same as restriction start.
        start_dt = datetime.now(timezone.utc)
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
    
class WriteTableWindowToGCS(DoFn):
    def __init__(self, bucket, dataset, file_prefix="output"):
        self.bucket = bucket
        self.dataset = dataset
        self.file_prefix = file_prefix

    def process(self, element, window=DoFn.WindowParam):
        table_name, records_iter = element
        records = list(records_iter)  # GroupByKey gives an iterable; materialize it

        window_start = window.start.to_utc_datetime().strftime('%Y%m%d-%H%M%S')
        window_end = window.end.to_utc_datetime().strftime('%Y%m%d-%H%M%S')
        path = f"gs://{self.bucket}/{self.dataset}/{table_name}/{self.file_prefix}-{window_start}-to-{window_end}.json"

        # Write NDJSON
        with filesystems.FileSystems.create(path) as fh:
            for rec in records:
                fh.write(json.dumps(rec._asdict(), default=str).encode('utf-8'))
                fh.write(b'\n')

        # Optionally yield the path or metadata
        yield path