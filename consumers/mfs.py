MAX_TOTAL=125
def _p_hist(x): return 25 if bool(x) else 0
def _p_sec(x):  return 15 if bool(x) else 0
def _p_aid(a):
    a=(a or '').lower()
    if a in ('furniture','clutching_furniture'): return 30
    if a in ('crutches','cane','walker','crutches/cane/walker'): return 15
    if a in ('none','bed','wheelchair','bed/wheelchair','none/bed/wheelchair','bedrest'): return 0
    return 0
def _p_iv(x):   return 20 if bool(x) else 0
def _p_gait(g):
    s=(g or '').lower()
    if s=='impaired': return 20
    if s=='weak':     return 10
    return 0
def _p_ms(m):
    s=(m or '').lower()
    return 15 if s in ('forgets_limitations','overestimates_ability','confused') else 0
def mfs_points(fields: dict) -> dict:
    return {
        'history_fall': _p_hist(fields.get('mfs_history_fall')),
        'secondary_dx': _p_sec(fields.get('mfs_secondary_dx')),
        'ambulatory_aid': _p_aid(fields.get('mfs_ambulatory_aid')),
        'iv_heparin': _p_iv(fields.get('mfs_iv_heparin')),
        'gait': _p_gait(fields.get('mfs_gait')),
        'mental_status': _p_ms(fields.get('mfs_mental_status')),
    }
def mfs_total_bucket(fields: dict) -> dict:
    pts=mfs_points(fields); total=sum(pts.values())
    bucket='HIGH' if total>=45 else ('MODERATE' if total>=25 else 'LOW')
    return {'mfs_points':pts,'mfs_total':int(total),'mfs_bucket':bucket,'risk_score':min(1.0,total/MAX_TOTAL)}
