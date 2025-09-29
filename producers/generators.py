import random
from datetime import datetime, timezone
from .mfs import mfs_total_bucket

def now_iso(): return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def random_mobility(resident_id, facility_id, unit_id, device_id):
    return {
        'event_type':'obs_mobility','ts_utc':now_iso(),'resident_id':resident_id,
        'facility_id':facility_id,'unit_id':unit_id,
        'room_id':f"{random.randint(100,399)}{random.choice(['A','B'])}",'source':'wearable',
        'device_id':device_id,'gait_speed_mps':round(random.uniform(0.3,1.2),2),
        'step_time_cv':round(random.uniform(0.05,0.35),2),'sit_to_stand_time_s':round(random.uniform(2.0,6.0),2),
        'recent_near_fall': random.random()<0.05
    }

def random_history(resident_id, facility_id, unit_id):
    return {
        'event_type':'history','ts_utc':now_iso(),'resident_id':resident_id,
        'facility_id':facility_id,'unit_id':unit_id,'room_id':f"{random.randint(100,399)}{random.choice(['A','B'])}",
        'source':'emr','prior_falls_12mo':random.choice([0,0,1,2]),
        'days_since_last_fall':random.randint(7,180),'cognitive_status':random.choice(['normal','mild','moderate','severe'])
    }

def random_environment(resident_id, facility_id, unit_id, staff_id):
    return {
        'event_type':'environment','ts_utc':now_iso(),'resident_id':resident_id,
        'facility_id':facility_id,'unit_id':unit_id,'room_id':f"{random.randint(100,399)}{random.choice(['A','B'])}",
        'source':'nurse_round','staff_id':staff_id,'lighting_lux':random.randint(5,300),
        'clutter':random.random()<0.2,'footwear_safe':random.random()>0.2,'bed_alarm_on':random.random()>0.2,
        'call_bell_within_reach':random.random()>0.2,'floor_wet':random.random()<0.05,
        'staff_to_resident_ratio':round(random.uniform(0.15,0.5),2),'time_of_day':random.choice(['day','evening','night'])
    }

def random_vitals(resident_id, facility_id, unit_id, staff_id):
    return {
        'event_type':'obs_vitals','ts_utc':now_iso(),'resident_id':resident_id,
        'facility_id':facility_id,'unit_id':unit_id,'room_id':f"{random.randint(100,399)}{random.choice(['A','B'])}",
        'source':'nurse_check','staff_id':staff_id,'hr_bpm':random.randint(55,110),
        'sbp_mmHg':random.randint(95,150),'dbp_mmHg':random.randint(55,95),'spo2_pct':random.randint(90,99),
        'orthostatic_drop_sys':random.random()<0.1
    }

def random_meds(resident_id, facility_id, unit_id):
    med=random.choice([
        ('Quetiapine','antipsychotic',True),('Trazodone','sedative',True),
        ('Lisinopril','antihypertensive',False),('Hydrocodone','opioid',True),('Metformin','hypoglycemic',False)
    ])
    return {
        'event_type':'meds','ts_utc':now_iso(),'resident_id':resident_id,
        'facility_id':facility_id,'unit_id':unit_id,'room_id':f"{random.randint(100,399)}{random.choice(['A','B'])}",
        'source':'emr','med_name':med[0],'med_class':med[1],'change_type':random.choice(['add','dose_change','stop']),
        'high_risk':bool(med[2])
    }

def random_mfs_assessment(resident_id, facility_id, unit_id, staff_id):
    history = random.random()<0.25
    secondary = random.random()<0.55
    aid = random.choices(['none','crutches/cane/walker','furniture'], weights=[6,3,1])[0]
    iv = random.random()<0.10
    gait = random.choices(['normal','weak','impaired'], weights=[6,3,1])[0]
    mental = random.choices(['oriented','forgets_limitations'], weights=[8,2])[0]
    base = {
        'event_type':'nurse_mfs','ts_utc':now_iso(),'resident_id':resident_id,
        'facility_id':facility_id,'unit_id':unit_id,'room_id':f"{random.randint(100,399)}{random.choice(['A','B'])}",
        'source':'nurse_check','staff_id':staff_id,
        'mfs_history_fall':history,'mfs_secondary_dx':secondary,'mfs_ambulatory_aid':aid,
        'mfs_iv_heparin':iv,'mfs_gait':gait,'mfs_mental_status':mental
    }
    scored = mfs_total_bucket(base)
    base.update(scored)
    return base
