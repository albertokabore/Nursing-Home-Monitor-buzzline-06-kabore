import threading, time
from collections import defaultdict
from typing import Dict, Callable, List

UNITS = ['U1','U2','U3']

def resident_unit(resident_id: str) -> str:
    try:
        n = int(resident_id[1:])
    except Exception:
        n = 1
    idx = (n-1)//10
    return UNITS[idx % len(UNITS)]

NURSE_BY_UNIT = {'U1':'nurse_u1','U2':'nurse_u2','U3':'nurse_u3'}
CNAS_BY_UNIT = {'U1':['cna_u1a','cna_u1b'],'U2':['cna_u2a','cna_u2b'],'U3':['cna_u3a','cna_u3b']}

def build_assignments(resident_ids: List[str]) -> Dict[str, Dict[str, List[str]]]:
    out = {u:{'nurse':[NURSE_BY_UNIT[u]], 'cna':CNAS_BY_UNIT[u][:], 'residents':[]} for u in UNITS}
    for rid in resident_ids:
        u = resident_unit(rid); out[u]['residents'].append(rid)
    return out

def is_active_shift(epoch: float, active_phases: List[str]) -> bool:
    lt = time.localtime(epoch); hour = lt.tm_hour
    if 'day' in active_phases and 7 <= hour < 19: return True
    if 'night' in active_phases and (hour >= 19 or hour < 7): return True
    return False

def rounding_loop(resident_ids: List[str], period_sec: int, active_phases: List[str], name_lookup: Callable[[str], str], on_alert: Callable[[str], None]):
    assignments = build_assignments(resident_ids)
    last_slot_by_resident: Dict[str, int] = defaultdict(lambda: -1)
    while True:
        now = time.time()
        if not is_active_shift(now, active_phases):
            time.sleep(1.0); continue
        slot = int(now // period_sec)
        for unit, blob in assignments.items():
            nurse = blob['nurse'][0]
            cnas = blob['cna']
            for i, rid in enumerate(blob['residents']):
                if last_slot_by_resident[rid] == slot: continue
                if slot % 2 == 0:
                    staff = nurse; role = 'Nurse'
                else:
                    staff = cnas[i % len(cnas)]; role = 'CNA'
                msg = f"ROUND NOW ({role}) unit={unit} staff={staff} resident={rid} name={name_lookup(rid)} due_slot={slot}"
                on_alert(msg)
                last_slot_by_resident[rid] = slot
        time.sleep(3.0)
