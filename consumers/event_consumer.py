import json, threading, queue, time, textwrap
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

import matplotlib
try:
    matplotlib.use('TkAgg', force=True)
except Exception:
    try: matplotlib.use('Qt5Agg', force=True)
    except Exception: pass
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

from .config import BOOTSTRAP, TOPIC, ROUNDING_PERIOD_SEC, ROUNDING_ACTIVE
from .mfs import mfs_total_bucket
from .risk import score as risk_score
from .rounding import rounding_loop

WINDOW_SECS=5*60
TOP_N=6
WINDOWS=defaultdict(lambda: deque())
CURRENT={}
NAME_BY_ID={}
Q=queue.Queue(maxsize=8000)
PROC_COUNT=0
LAST_STAT_TS=time.time()

LAST_BUCKET={}           # rid -> bucket
LAST_ALERT_TS={}         # (rid,bucket) -> last ts
ALERT_LOG=deque(maxlen=8)
ROUND_LOG=deque(maxlen=8)

def _parse_ts(ts):
    try: return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception: return datetime.now(timezone.utc)

def color_for_score(s: float)->str:
    if s>=45/125: return 'red'
    if s>=25/125: return 'orange'
    return 'green'

def _seek_to_live_tail(c: KafkaConsumer)->None:
    c.poll(timeout_ms=0)
    while not c.assignment():
        c.poll(timeout_ms=50)
    parts=list(c.assignment())
    if parts: c.seek_to_end(*parts)

def poll_thread():
    c=KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP.split(','),
        enable_auto_commit=False,
        auto_offset_reset='latest',
        max_poll_records=1000,
        fetch_min_bytes=1,
        fetch_max_wait_ms=25,
    )
    _seek_to_live_tail(c)
    while True:
        recs=c.poll(timeout_ms=50, max_records=1000)
        for _, msgs in recs.items():
            for m in msgs:
                try: Q.put_nowait(m)
                except queue.Full:
                    try: Q.get_nowait(); Q.put_nowait(m)
                    except queue.Empty: pass

def _prune_window(rid, now_utc):
    cutoff=now_utc - timedelta(seconds=WINDOW_SECS)
    dq=WINDOWS[rid]
    while dq and _parse_ts(dq[0][0]) < cutoff:
        dq.popleft()

def _drivers(f):
    d=[]
    aid=(f.get('mfs_ambulatory_aid') or '').lower()
    gait=(f.get('mfs_gait') or '').lower()
    ms=(f.get('mfs_mental_status') or '').lower()
    if f.get('mfs_history_fall'): d.append('history_fall')
    if f.get('mfs_secondary_dx'): d.append('secondary_dx')
    if aid=='furniture': d.append('aid=furniture')
    elif aid in ('crutches','cane','walker','crutches/cane/walker'): d.append('aid=assistive')
    if f.get('mfs_iv_heparin'): d.append('iv/heparin')
    if gait in ('weak','impaired'): d.append(f'gait={gait}')
    if ms in ('forgets_limitations','overestimates_ability','confused'): d.append('cognitive_limits')
    if f.get('recent_near_fall'): d.append('near_fall')
    if f.get('gait_speed_mps',1.0) < 0.6: d.append('slow_gait')
    if f.get('step_time_cv',0.0) > 0.2: d.append('step_variability')
    if f.get('sit_to_stand_time_s',2.0) > 4.0: d.append('slow_sts')
    if f.get('orthostatic_drop_sys'): d.append('orthostatic_drop')
    if f.get('floor_wet'): d.append('floor_wet')
    if f.get('clutter'): d.append('clutter')
    if not f.get('footwear_safe', True): d.append('footwear')
    if not f.get('bed_alarm_on', False): d.append('bed_alarm_off')
    if not f.get('call_bell_within_reach', True): d.append('call_bell')
    if f.get('high_risk'): d.append('high_risk_med')
    return d[:4] if len(d)>4 else d

def _maybe_alert(rid, name, score, info):
    bucket = info.get('mfs',{}).get('mfs_bucket') or ('HIGH' if score>=45/125 else ('MODERATE' if score>=25/125 else 'LOW'))
    prev = LAST_BUCKET.get(rid)
    now=time.time()
    if (prev != bucket) or (now - LAST_ALERT_TS.get((rid,bucket),0) > 20):
        if bucket in ('HIGH','MODERATE'):
            label='ALERT HIGH' if bucket=='HIGH' else 'ALERT WATCH'
            drivers='/'.join(info.get('drivers', [])) or 'n/a'
            msg=f"{label} resident={rid} name={name} score={score:.2f} drivers={drivers}"
            print(msg)
            ALERT_LOG.appendleft(msg)
            LAST_ALERT_TS[(rid,bucket)]=now
    LAST_BUCKET[rid]=bucket

def update_state(batch_limit=1500):
    global PROC_COUNT
    processed=0
    while processed<batch_limit:
        try: m=Q.get_nowait()
        except queue.Empty: break
        processed+=1; PROC_COUNT+=1
        try: payload=json.loads(m.value.decode('utf-8'))
        except Exception: continue
        rid=payload.get('resident_id'); ts=payload.get('ts_utc') or payload.get('ts')
        if not rid or not ts: continue
        if payload.get('resident_name'): NAME_BY_ID[rid]=payload['resident_name']
        WINDOWS[rid].append((ts,payload))
        _prune_window(rid, _parse_ts(ts))
        merged={}
        for _,f in WINDOWS[rid]: merged.update(f)
        drivers=_drivers(merged)
        if 'mfs_total' in merged and 'mfs_points' in merged:
            total=int(merged['mfs_total'])
            calc=mfs_total_bucket(merged)
            if calc['mfs_total']!=total:
                print(f"[WARN] MFS mismatch {rid}: event={total} calc={calc['mfs_total']}")
                total=calc['mfs_total']
            CURRENT[rid]={'score':min(1.0,total/125.0),'mfs':{'mfs_total':total,'mfs_bucket':calc['mfs_bucket']},'ts':ts,'drivers':drivers}
        else:
            have_mfs=all(k in merged for k in ('mfs_history_fall','mfs_secondary_dx','mfs_ambulatory_aid','mfs_iv_heparin','mfs_gait','mfs_mental_status'))
            out=mfs_total_bucket(merged) if have_mfs else risk_score(merged)
            if 'mfs_total' in out:
                CURRENT[rid]={'score':out['risk_score'],'mfs':{'mfs_total':out['mfs_total'],'mfs_bucket':out['mfs_bucket']},'ts':ts,'drivers':drivers}
            else:
                CURRENT[rid]={'score':out['risk_score'],'ts':ts,'drivers':drivers}
        _maybe_alert(rid, NAME_BY_ID.get(rid,rid), CURRENT[rid]['score'], CURRENT[rid])

def wrap_name(name: str, width: int=18)->str:
    if len(name)<=width: return name
    return '\n'.join(textwrap.wrap(name, width=width, break_long_words=False, max_lines=2, placeholder='…'))

# ---- Rounding integration ----
ROUND_LOG = ROUND_LOG  # keep global
def start_rounding_thread():
    resident_ids=list(CURRENT.keys()) or [f'R{i:03d}' for i in range(1,31)]
    def name_lookup(rid): return NAME_BY_ID.get(rid, rid)
    def on_alert(msg):
        print(msg)
        ROUND_LOG.appendleft(msg)
    t=threading.Thread(target=rounding_loop, args=(resident_ids, int(ROUNDING_PERIOD_SEC), list(ROUNDING_ACTIVE), name_lookup, on_alert), daemon=True)
    t.start()

def main():
    threading.Thread(target=poll_thread, daemon=True).start()
    start_rounding_thread()

    fig,ax=plt.subplots(figsize=(14,7.5), dpi=100)
    fig.subplots_adjust(left=0.30, right=0.90, top=0.90, bottom=0.14)
    bar_labels=[]; bar_patches=[]; bar_texts=[]; avg_line=[None]

    def draw_bars(labels, vals, cols):
        nonlocal bar_patches, bar_texts
        ax.clear(); ax.set_xlim(0,1)
        ax.set_title('Live fall risk for residents last five minutes')
        ax.set_xlabel('Risk score'); ax.tick_params(axis='y', labelsize=11)
        container=ax.barh(labels, vals, color=cols)
        bar_patches=list(container)
        bar_texts=[ax.text(min(1.0,v+0.01), r.get_y()+r.get_height()/2, f'{v:.2f}', va='center') for r,v in zip(bar_patches, vals)]

    def animate(_):
        nonlocal bar_labels, bar_patches, bar_texts
        global PROC_COUNT, LAST_STAT_TS
        update_state()
        now=time.time()
        if now - LAST_STAT_TS >= 1.0:
            print(f"consumer: {PROC_COUNT:4d} msgs/s | residents: {len(CURRENT)}"); PROC_COUNT=0; LAST_STAT_TS=now

        items=sorted(CURRENT.items(), key=lambda kv: kv[1]['score'], reverse=True)[:TOP_N]
        if not items:
            ax.set_xlim(0,1); ax.set_title('Waiting for live events…'); return
        raw=[NAME_BY_ID.get(rid,rid) for rid,_ in items]
        labels=[wrap_name(n,18) for n in raw]
        vals=[v['score'] for _,v in items]
        cols=[color_for_score(s) for s in vals]
        longest=max(len(n.replace('\n','')) for n in labels)
        fig.subplots_adjust(left=min(0.62, 0.24 + 0.012*longest), right=0.90, top=0.90, bottom=0.14)

        if labels!=bar_labels or len(bar_patches)!=len(labels):
            bar_labels=labels; draw_bars(labels, vals, cols)
        else:
            for rect,val,col in zip(bar_patches, vals, cols): rect.set_width(val); rect.set_color(col)
            for txt,val,(_,data) in zip(bar_texts, vals, items):
                txt.set_text(f"{int(data['mfs']['mfs_total'])}/125" if data.get('mfs') else f"{val:.2f}")
                y=txt.get_position()[1]; txt.set_position((min(1.0, val+0.01), y))

        total=len(CURRENT); avg=(sum(v['score'] for v in CURRENT.values())/total) if total else 0.0
        if avg_line[0] is None: avg_line[0]=ax.axvline(avg, linestyle='--')
        else: avg_line[0].set_xdata([avg,avg])
        mod=sum(1 for v in CURRENT.values() if 25/125 <= v['score'] < 45/125)
        high=sum(1 for v in CURRENT.values() if v['score'] >= 45/125)

        lines=[f"Residents: {total}", f"High risk: {high}", f"Watch: {mod}", f"Average: {avg:.2f}", "", "Recent alerts:"]
        if ALERT_LOG:
            for s in list(ALERT_LOG)[:3]:
                if len(s)>58: s=s[:55]+'…'
                lines.append('• '+s)
        else:
            lines.append('• (none yet)')
        lines.extend(["", "Rounding now (due):"])
        if ROUND_LOG:
            for s in list(ROUND_LOG)[:3]:
                if len(s)>58: s=s[:55]+'…'
                lines.append('• '+s)
        else:
            lines.append('• (none yet)')

        # Build once, then pass to ax.text to avoid unterminated string errors
        panel_text = "\n".join(lines)
        ax.text(1.005, 0.98, panel_text, transform=ax.transAxes, va='top', ha='left',
                bbox=dict(facecolor='white', edgecolor='gray', boxstyle='round,pad=0.35'))

        return bar_patches + bar_texts + ([avg_line[0]] if avg_line[0] else [])

    global anim
    anim=FuncAnimation(fig, animate, interval=120, cache_frame_data=False)
    plt.tight_layout(); plt.show()

if __name__=='__main__':
    main()
