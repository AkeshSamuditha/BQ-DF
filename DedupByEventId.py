import apache_beam as beam
from apache_beam.transforms.core import DoFn
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.userstate import MapStateSpec
import json
class DedupByEventId(DoFn):

    seen = MapStateSpec("seen", StrUtf8Coder(), StrUtf8Coder())

    def process(self, element, seen=DoFn.StateParam(seen)):
        record = json.loads(element)
        event_id = record["event_id"]

        if seen.get(event_id) is not None:
            return

        seen.put(event_id, "1")
        yield element