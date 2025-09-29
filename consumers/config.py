import os
from dotenv import load_dotenv
load_dotenv()
BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
TOPIC=os.getenv('KAFKA_TOPIC','fall_risk_events')
ROUNDING_PERIOD_SEC=int(os.getenv('ROUNDING_PERIOD_SEC','3600'))
ROUNDING_ACTIVE=[p.strip().lower() for p in os.getenv('ROUNDING_ACTIVE','day,night').split(',') if p.strip()]
