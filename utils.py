from datetime import datetime, timezone
from config import PRECISION

def convert_macros_to_seconds(microseconds: int) -> float:
    """Convert microseconds to seconds as float"""
    return microseconds / PRECISION

def convert_seconds_to_macros(seconds: float) -> int:
    """Convert seconds as float to microseconds"""
    return int(seconds * PRECISION)

def convert_macros_to_ts(microseconds: int) -> int:
    """
    Docstring for convert_macros_to_ts
    :param microseconds: Microseconds since epoch
    :type microseconds: int
    :return: Timestamp string in 'YYYY-MM-DD HH:MM:SS.ffffff' format
    :rtype: str
    """
    seconds = convert_macros_to_seconds(microseconds)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    ts = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    return ts
    

def simulate_cpu_work(self):
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
