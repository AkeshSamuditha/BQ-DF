import apache_beam as beam
from apache_beam.transforms.core import DoFn
from apache_beam.coders import  BooleanCoder
from apache_beam.transforms.userstate import  ReadModifyWriteStateSpec
import json
import typing
from bq_common import BQRecord

# Note: MapStateSpec is not available in Python Beam SDK
# Use ReadModifyWriteStateSpec with keyed input instead

class DeduplicateDoFn(DoFn):
    """Stateful DoFn to detect duplicates across bundles.
    
    Requires keyed input: (key, BQRecord)
    Key should be unique identifier like f"{table_name}:{event_id}"
    """
    
    SEEN_STATE = ReadModifyWriteStateSpec('seen', BooleanCoder())
    
    def process(
        self,
        element: typing.Tuple[str, BQRecord],
        seen_state=DoFn.StateParam(SEEN_STATE)
    ) -> typing.Iterable[BQRecord]:
        
        # Unpack the keyed element
        key, record = element
        
        # Check if we've seen this event before
        is_duplicate = seen_state.read() or False
        
        # Mark as seen for future
        if not is_duplicate:
            seen_state.write(True)
        
        # Yield record with duplicate flag
        yield BQRecord(
            table_name=record.table_name,
            event_id=record.event_id,
            change_type=record.change_type,
            payload=record.payload,
            change_timestamp=record.change_timestamp,
            is_late=record.is_late,
            is_duplicate=is_duplicate
        )