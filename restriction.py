import typing
from typing import Tuple
import logging
import sys
from datetime import datetime, timezone
from apache_beam.transforms.core import RestrictionProvider
from config import PRECISION

from apache_beam.io.iobase import RestrictionProgress
from apache_beam.io.iobase import RestrictionTracker

logger = logging.getLogger(__name__)

class BQTableConfig(typing.NamedTuple):
    table_name: str
    poll_interval: int = 30

class BQRecord(typing.NamedTuple):
    event_id: str
    change_type: str
    change_timestamp: str
    table_name: str
    payload: typing.Dict[str, typing.Any]

# class BQCDCRestriction(typing.NamedTuple):
#     """Custom restriction that tracks query start position"""
#     start: int
#     stop: int
#     prev_succefull_claim: int  # Stores the previous successful claim

#     def size(self):
#         return self.stop - self.start
    
class BQCDCRestrictionRange(object):
  def __init__(self, start, stop, prev_succefull_claim=None):
    if start > stop:
      raise ValueError(
          'Start offset must be not be larger than the stop offset. '
          'Received %d and %d respectively.' % (start, stop))
    self.start = start
    self.stop = stop
    self.prev_succefull_claim = prev_succefull_claim if prev_succefull_claim is not None else start

  def __eq__(self, other):
    if not isinstance(other, BQCDCRestrictionRange):
      return False

    return self.start == other.start and self.stop == other.stop and self.prev_succefull_claim == other.prev_succefull_claim

  def __hash__(self):
    return hash((type(self), self.start, self.stop, self.prev_succefull_claim))

  def __repr__(self):
    return 'BQCDCRestrictionRange(start=%s, stop=%s, prev_succefull_claim=%s)' % (self.start, self.stop, self.prev_succefull_claim) 

  def split(self, desired_num_offsets_per_split, min_num_offsets_per_split=1):
    current_split_start = self.start
    max_split_size = max(
        desired_num_offsets_per_split, min_num_offsets_per_split)
    while current_split_start < self.stop:
      current_split_stop = min(current_split_start + max_split_size, self.stop)
      remaining = self.stop - current_split_stop

      # Avoiding a small split at the end.
      if (remaining < desired_num_offsets_per_split // 4 or
          remaining < min_num_offsets_per_split):
        current_split_stop = self.stop

      yield BQCDCRestrictionRange(current_split_start, current_split_stop)
      current_split_start = current_split_stop

  def split_at(self, split_pos, last_claimed_pos) -> Tuple['BQCDCRestrictionRange', 'BQCDCRestrictionRange']:
    primary = BQCDCRestrictionRange(start = self.start, stop = split_pos, prev_succefull_claim=self.prev_succefull_claim)
    
    new_prev_succefull_claim = last_claimed_pos if last_claimed_pos is not None else self.start
    
    residual = BQCDCRestrictionRange(start = split_pos, stop = self.stop, prev_succefull_claim=new_prev_succefull_claim)
    return primary, residual

  def new_tracker(self):
    return BQCDCRestrictionTracker(self.start, self.stop, self.prev_succefull_claim)

  def size(self):
    return self.stop - self.start


class BQCDCRestrictionTracker(RestrictionTracker):
  """An `iobase.RestrictionTracker` implementations for an offset range.

  Offset range is represented as BQCDCRestrictionRange.
  """
  def __init__(self, offset_range: BQCDCRestrictionRange) -> None:
    assert isinstance(offset_range, BQCDCRestrictionRange), offset_range
    self._range = offset_range
    self._current_position = None
    self._last_claim_attempt = None
    self._checkpointed = False

  def check_done(self):
    if (self._range.start != self._range.stop and
        (self._last_claim_attempt is None or
         self._last_claim_attempt < self._range.stop - 1)):
      raise ValueError(
          'BQCDCRestrictionTracker is not done since work in range [%s, %s) '
          'has not been claimed.' % (
              self._last_claim_attempt
              if self._last_claim_attempt is not None else self._range.start,
              self._range.stop))

  def current_restriction(self):
    return self._range

  def current_progress(self) -> RestrictionProgress:
    if self._current_position is None:
      fraction = 0.0
    elif self._range.stop == self._range.start:
      # If self._current_position is not None, we must be done.
      fraction = 1.0
    else:
      fraction = (
          float(self._current_position - self._range.start) /
          (self._range.stop - self._range.start))
    return RestrictionProgress(fraction=fraction)

  def start_position(self):
    return self._range.start

  def stop_position(self):
    return self._range.stop

  def try_claim(self, position):
    if (self._last_claim_attempt is not None and position <= self._last_claim_attempt):
      raise ValueError(
          'Positions claimed should strictly increase. Trying to claim '
          'position %d while last claim attempt was %d.' %
          (position, self._last_claim_attempt))

    self._last_claim_attempt = position
    if position < self._range.start:
      raise ValueError(
          'Position to be claimed cannot be smaller than the start position '
          'of the range. Tried to claim position %r for the range [%r, %r)' %
          (position, self._range.start, self._range.stop))

    if self._range.start <= position < self._range.stop:
        # update prev_succefull_claim to current position or start if current is None
        self._range.prev_succefull_claim = self._current_position if self._current_position is not None else self._range.start
        self._current_position = position
      
        return True

    return False

  def try_split(self, fraction_of_remainder):
    if not self._checkpointed:
      if self._last_claim_attempt is None:
        cur = self._range.start - 1
      else:
        cur = self._last_claim_attempt
      split_point = (
          cur + int(max(1, (self._range.stop - cur) * fraction_of_remainder)))
      if split_point < self._range.stop:
        if fraction_of_remainder == 0:
          self._checkpointed = True
        self._range, residual_range = self._range.split_at(split_point, self._current_position)
        return self._range, residual_range

  def is_bounded(self):
    return False

class BQCDCRestrictionProvider(RestrictionProvider):    
    def initial_restriction(self, element: BQTableConfig) -> BQCDCRestrictionRange:
        """Initialize the restriction"""
        start_dt = datetime.now(timezone.utc)
        start_micros = int(start_dt.timestamp() * PRECISION)

        # Set end time to max value for unbounded
        end_micros = sys.maxsize
        return BQCDCRestrictionRange(start=start_micros, stop=end_micros, prev_succefull_claim=start_micros)


    def create_tracker(self, restriction: BQCDCRestrictionRange) -> BQCDCRestrictionTracker:
        """Create restriction tracker."""
        return BQCDCRestrictionTracker(restriction)

    def restriction_size(self, element: typing.Any, restriction: BQCDCRestrictionRange) -> int:
        """Calculate size of restriction"""
        return restriction.size()

    def split(self, element: typing.Any, restriction: BQCDCRestrictionRange) -> typing.Iterable[BQCDCRestrictionRange]:
        """Split restriction - no splitting for streaming"""
        return [restriction]