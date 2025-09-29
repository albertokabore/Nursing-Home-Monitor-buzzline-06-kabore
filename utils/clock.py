import time
def precise_sleep_until(next_ts: float):
    delay = next_ts - time.perf_counter()
    if delay > 0:
        time.sleep(delay)
    else:
        return time.perf_counter()
    return next_ts
