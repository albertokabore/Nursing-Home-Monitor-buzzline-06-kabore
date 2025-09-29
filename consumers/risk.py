from typing import Dict
def sigmoid(x):
    import math; return 1/(1+math.exp(-x))
def score(features: Dict[str,float]) -> Dict[str,float]:
    gait=features.get('gait_speed_mps',1.0); gait_inv=max(0.0,min(1.0,(1.4-gait)/(1.4-0.2)))
    stcv=features.get('step_time_cv',0.0);   stcv=max(0.0,min(1.0,(stcv-0.0)/(0.5-0.0)))
    sts=features.get('sit_to_stand_time_s',2.0); sts=max(0.0,min(1.0,(sts-2.0)/(8.0-2.0)))
    nf=1.0 if features.get('recent_near_fall') else 0.0
    lin=0.9*gait_inv+0.7*stcv+0.5*sts+1.0*nf-1.5
    return {'risk_score':sigmoid(lin)}
