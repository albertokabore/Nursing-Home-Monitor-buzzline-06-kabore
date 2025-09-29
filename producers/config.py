import os
from dotenv import load_dotenv
load_dotenv()
BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
TOPIC=os.getenv('KAFKA_TOPIC','fall_risk_events')
SEND_RATE_HZ=float(os.getenv('SEND_RATE_HZ','12'))
RESIDENT_COUNT=int(os.getenv('RESIDENT_COUNT','30'))
FACILITY_ID=os.getenv('FACILITY_ID','FAC01')
UNITS=[u.strip() for u in os.getenv('UNITS','U1,U2,U3').split(',')]
