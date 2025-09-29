import json, random, time
from kafka import KafkaProducer
from .config import BOOTSTRAP, TOPIC, SEND_RATE_HZ, RESIDENT_COUNT, FACILITY_ID, UNITS
from .generators import random_mobility, random_history, random_environment, random_vitals, random_meds, random_mfs_assessment
from .roster import RESIDENTS as ROSTER
from utils.clock import precise_sleep_until

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: v.encode('utf-8'),
        linger_ms=0, acks=0
    )
    roster = ROSTER[:RESIDENT_COUNT]
    residents = [r['id'] for r in roster]
    name_by_id = {r['id']: r['name'] for r in roster}
    seq = {r:0 for r in residents}
    counters = {r:0 for r in residents}
    idx = 0
    period = max(0.005, 1.0/max(0.1, SEND_RATE_HZ))
    print(f'Sending to {TOPIC} on {BOOTSTRAP}. Ctrl+C to stop.')
    try:
        next_tick = time.perf_counter()
        while True:
            r = residents[idx]; idx = (idx+1)%len(residents)
            counters[r]+=1; seq[r]+=1
            unit = random.choice(UNITS)
            k = counters[r] % 12
            if k in (0,2,4,6,8): e = random_mobility(r, FACILITY_ID, unit, device_id=f'dev_{r}')
            elif k in (1,7):    e = random_environment(r, FACILITY_ID, unit, staff_id='nurse_01')
            elif k in (3,9):    e = random_vitals(r, FACILITY_ID, unit, staff_id='nurse_01')
            elif k == 5:        e = random_meds(r, FACILITY_ID, unit)
            elif k == 10:       e = random_mfs_assessment(r, FACILITY_ID, unit, staff_id='nurse_01')
            else:               e = random_history(r, FACILITY_ID, unit)
            e['resident_name'] = name_by_id.get(r, r)
            e['schema_version'] = '1.0'; e['seq'] = seq[r]; e['ingest_ts_utc'] = e['ts_utc']
            producer.send(TOPIC, key=r, value=e); producer.flush()
            if seq[r] % 25 == 0:
                print(f'sent {seq[r]:>4} events for {r} ({name_by_id[r]})')
            next_tick += period
            maybe = precise_sleep_until(next_tick)
            if maybe is not None and maybe != next_tick:
                next_tick = maybe
    except KeyboardInterrupt:
        print('Stopped.')

if __name__=='__main__':
    main()
