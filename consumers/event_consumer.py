# consumers/event_consumer.py
import json, threading, queue, time, textwrap, math
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

# ------------------------ constants ------------------------
WINDOW_SECS = 5 * 60
TOP_N = 6

# Move panel leftward to make it wider (more room for messages)
PANEL_LEFT = 0.58          # was 0.66
PANEL_TOP = 0.92
PANEL_BOTTOM = 0.12
PANEL_PAD_RIGHT = 0.02

ALERT_MAX_CHARS = 84
ALERT_SHOW_MAX = 16

# ------------------------ state ----------------------------
WINDOWS = defaultdict(lambda: deque())
CURRENT = {}
NAME_BY_ID = {}
Q = queue.Queue(maxsize=8000)
PROC_COUNT = 0
LAST_STAT_TS = time.time()

LAST_BUCKET = {}
LAST_ALERT_TS = {}
ALERT_LOG = deque(maxlen=240)
ROUND_LOG = deque(maxlen=400)

# ------------------------ helpers --------------------------
def _parse_ts(ts):
    try: return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception: return datetime.now(timezone.utc)

def color_for_score(s: float)->str:
    if s>=45/125: return '#e53935'
    if s>=25/125: return '#fb8c00'
    return '#43a047'

def _seek_to_live_tail(c: KafkaConsumer)->None:
    c.poll(timeout_ms=0)
    while not c.assignment(): c.poll(timeout_ms=50)
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
    d=[]; aid=(f.get('mfs_ambulatory_aid') or '').lower()
    gait=(f.get('mfs_gait') or '').lower(); ms=(f.get('mfs_mental_status') or '').lower()
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
    if f.get('sit_to-stand_time_s', f.get('sit_to_stand_time_s', 2.0)) > 4.0: d.append('slow_sts')
    if f.get('orthostatic_drop_sys'): d.append('orthostatic_drop')
    if f.get('floor_wet'): d.append('floor_wet')
    if f.get('clutter'): d.append('clutter')
    if not f.get('footwear_safe', True): d.append('footwear')
    if not f.get('bed_alarm_on', False): d.append('bed_alarm_off')
    if not f.get('call_bell_within_reach', True): d.append('call_bell')
    if f.get('high_risk'): d.append('high_risk_med')
    return d[:4] if len(d)>4 else d

def _friendly_alert(label, name, score, drivers):
    d = '/'.join(drivers) if drivers else 'n/a'
    return f"{label} — {name} ({score:.2f}) — {d}"

def _maybe_alert(rid, name, score, info):
    bucket = info.get('mfs',{}).get('mfs_bucket') or ('HIGH' if score>=45/125 else ('MODERATE' if score>=25/125 else 'LOW'))
    prev = LAST_BUCKET.get(rid); now=time.time()
    if (prev != bucket) or (now - LAST_ALERT_TS.get((rid,bucket),0) > 20):
        if bucket in ('HIGH','MODERATE'):
            label='ALERT HIGH' if bucket=='HIGH' else 'ALERT WATCH'
            pretty=_friendly_alert(label, name, score, info.get('drivers', []))
            print(pretty)
            ALERT_LOG.appendleft(pretty)
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
        merged={}; [merged.update(f) for _,f in WINDOWS[rid]]
        drivers=_drivers(merged)
        if 'mfs_total' in merged and 'mfs_points' in merged:
            total=int(merged['mfs_total']); calc=mfs_total_bucket(merged)
            if calc['mfs_total']!=total:
                print(f"[WARN] MFS mismatch {rid}: event={total} calc={calc['mfs_total']}")
                total=calc['mfs_total']
            CURRENT[rid]={'score':min(1.0,total/125.0),'mfs':{'mfs_total':total,'mfs_bucket':calc['mfs_bucket']},'ts':ts,'drivers':drivers}
        else:
            have=all(k in merged for k in ('mfs_history_fall','mfs_secondary_dx','mfs_ambulatory_aid','mfs_iv_heparin','mfs_gait','mfs_mental_status'))
            out=mfs_total_bucket(merged) if have else risk_score(merged)
            if 'mfs_total' in out:
                CURRENT[rid]={'score':out['risk_score'],'mfs':{'mfs_total':out['mfs_total'],'mfs_bucket':out['mfs_bucket']},'ts':ts,'drivers':drivers}
            else:
                CURRENT[rid]={'score':out['risk_score'],'ts':ts,'drivers':drivers}
        _maybe_alert(rid, NAME_BY_ID.get(rid,rid), CURRENT[rid]['score'], CURRENT[rid])

def wrap_name(name: str, width: int=18)->str:
    if len(name)<=width: return name
    return '\n'.join(textwrap.wrap(name, width=width, break_long_words=False, max_lines=2, placeholder='…'))

# --------- shifts & time watch ----------
def shift_label(epoch=None) -> str:
    if epoch is None: epoch = time.time()
    hr = time.localtime(epoch).tm_hour
    if 7 <= hr < 15:  return "Day (07–15)"
    if 15 <= hr < 23: return "Evening (15–23)"
    return "Night (23–07)"

def time_watch(period: int) -> str:
    now = time.time()
    local = datetime.fromtimestamp(now).strftime("%Y-%m-%d %I:%M:%S %p")
    next_slot = (math.floor(now/period)+1)*period
    left = int(round(next_slot - now))
    mm, ss = divmod(max(0,left), 60); hh, mm = divmod(mm, 60)
    return f"Local time: {local} | Shift: {shift_label(now)} | Next rounding in {hh:02d}:{mm:02d}:{ss:02d}"

# ---- Rounding integration ----
ROUND_LOG = ROUND_LOG
def start_rounding_thread():
    resident_ids=list(CURRENT.keys()) or [f'R{i:03d}' for i in range(1,31)]
    def name_lookup(rid): return NAME_BY_ID.get(rid, rid)
    def on_alert(msg): print(msg); ROUND_LOG.appendleft(msg)
    t=threading.Thread(target=rounding_loop, args=(resident_ids, int(ROUNDING_PERIOD_SEC), list(ROUNDING_ACTIVE), name_lookup, on_alert), daemon=True)
    t.start()

def _latest_rounding_lines():
    if not ROUND_LOG: return ["• (none yet)"]
    parsed=[]; latest=None
    for s in list(ROUND_LOG):
        if "ROUND NOW" not in s or "due_slot=" not in s: continue
        toks = {kv.split('=')[0]: kv.split('=')[1] for kv in s.split() if '=' in kv}
        slot = int(toks.get('due_slot','-1')); unit = toks.get('unit','?')
        role = 'Nurse' if 'Nurse' in s else ('CNA' if 'CNA' in s else '?')
        staff = toks.get('staff','?'); rid = toks.get('resident','?')
        parsed.append((slot, unit, role, staff, rid))
        latest = slot if (latest is None or slot>latest) else latest
    if latest is None: return ["• (none yet)"]
    by_ur=defaultdict(lambda: defaultdict(list))
    staff_lbl=defaultdict(lambda: defaultdict(set))
    for slot,unit,role,staff,rid in parsed:
        if slot!=latest: continue
        by_ur[unit][role].append(rid); staff_lbl[unit][role].add(staff)
    lines=[f"Rounding (latest slot) — {shift_label()}"]
    for unit in sorted(by_ur.keys()):
        for role in ("Nurse","CNA"):
            rids=by_ur[unit].get(role,[])
            if not rids: continue
            staff_str="/".join(sorted(staff_lbl[unit][role]))
            lines.append(f"{unit} {role} ({staff_str}): {','.join(sorted(rids))}")
    return lines if len(lines)>1 else ["• (none yet)"]

# ------------------------ main / viz -----------------------
def main():
    threading.Thread(target=poll_thread, daemon=True).start()
    start_rounding_thread()

    # Wider panel + compact figure (wider overall width to breathe)
    fig = plt.figure(figsize=(12.4, 6.1), dpi=120)
    ax = fig.add_axes([0.22, PANEL_BOTTOM, PANEL_LEFT-0.035, PANEL_TOP-PANEL_BOTTOM])
    panel_ax = fig.add_axes([PANEL_LEFT, PANEL_BOTTOM, 1.0 - PANEL_LEFT - PANEL_PAD_RIGHT, PANEL_TOP - PANEL_BOTTOM])
    panel_ax.axis('off')

    bar_labels=[]; bar_patches=[]; bar_texts=[]; avg_line=[None]

    def draw_bars(labels, vals, cols):
        nonlocal bar_patches, bar_texts
        ax.clear(); ax.set_xlim(0,1)
        ax.set_title('Live fall risk for residents last five minutes', fontsize=18, fontweight='bold')
        ax.set_xlabel('Risk score'); ax.tick_params(axis='y', labelsize=10)
        container=ax.barh(labels, vals, color=cols, edgecolor='white', linewidth=0.5)
        bar_patches=list(container)
        bar_texts=[ax.text(min(1.0,v+0.01), r.get_y()+r.get_height()/2, f'{v:.2f}', va='center', fontsize=9)
                   for r,v in zip(bar_patches, vals)]

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

        # left margin adapts to longest label, but keeps more room for panel
        longest=max(len(n.replace('\n','')) for n in labels)
        left = min(0.50, 0.20 + 0.010*longest)   # allow chart to sit further left
        ax.set_position([left, PANEL_BOTTOM, PANEL_LEFT-0.035-left+0.001, PANEL_TOP-PANEL_BOTTOM])

        if labels!=bar_labels or len(bar_patches)!=len(labels):
            bar_labels=labels; draw_bars(labels, vals, cols)
        else:
            for rect,val,col in zip(bar_patches, vals, cols): rect.set_width(val); rect.set_color(col)
            for txt,val,(_,data) in zip(bar_texts, vals, items):
                txt.set_text(f"{int(data['mfs']['mfs_total'])}/125" if data.get('mfs') else f"{val:.2f}")
                y=txt.get_position()[1]; txt.set_position((min(1.0, val+0.01), y))

        total=len(CURRENT); avg=(sum(v['score'] for v in CURRENT.values())/total) if total else 0.0
        if avg_line[0] is None: avg_line[0]=ax.axvline(avg, linestyle='--', color='#555')
        else: avg_line[0].set_xdata([avg,avg])
        mod=sum(1 for v in CURRENT.values() if 25/125 <= v['score'] < 45/125)
        high=sum(1 for v in CURRENT.values() if v['score'] >= 45/125)

        # -------- right panel (single wide column) --------
        def _wrap(s, width=ALERT_MAX_CHARS): return textwrap.fill(s, width=width, break_long_words=False)
        lines = [
            time_watch(int(ROUNDING_PERIOD_SEC)),
            f"Residents: {total}   High: {high}   Watch: {mod}   Avg: {avg:.2f}",
            "",
            "Rounding:"
        ]
        for s in _latest_rounding_lines(): lines.append("• " + _wrap(s))
        lines += ["", "Recent alerts:"]
        if ALERT_LOG:
            for s in list(ALERT_LOG)[:ALERT_SHOW_MAX]:
                lines.append("• " + _wrap(s))
        else:
            lines.append("• (none yet)")

        panel_text = "\n".join(lines)
        lc = panel_text.count("\n")+1
        fsize = 10
        if lc > 24: fsize = 9
        if lc > 32: fsize = 8
        if lc > 40: fsize = 7

        panel_ax.cla(); panel_ax.axis('off')
        panel_ax.add_patch(plt.Rectangle((0,0), 1, 1, facecolor='#f8f9fb', edgecolor='#bdbdbd', lw=1))
        panel_ax.text(0.02, 0.98, panel_text, ha='left', va='top', fontsize=fsize)

        return bar_patches + bar_texts + ([avg_line[0]] if avg_line[0] else [])

    global anim
    anim=FuncAnimation(fig, animate, interval=150, cache_frame_data=False)
    plt.show()

if __name__=='__main__':
    main()
