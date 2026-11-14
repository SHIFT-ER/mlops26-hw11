"""Сводка опыта C: моменты HPA 2→3→2, появление и готовность Pod, алерты; сравнение с фазами нагрузки."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from kexp import read_jsonl, summarize  # noqa: E402


def k8s_ts(s):
    return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp()


def stats(d, phases):
    tl = read_jsonl(d / 'timeline.jsonl')
    ev = read_jsonl(d / 'events.jsonl')
    recs = []
    for f in d.glob('requests-*.jsonl'):
        recs += read_jsonl(f)
    t0 = min(r['scheduled_wall'] for r in recs)
    rel = lambda t: None if t is None else round(t - t0, 1)  # noqa: E731
    out = dict(load_phases=phases, summary=summarize(recs))
    prev, changes = None, []
    for s in tl:
        h = s.get('hpa') or {}
        if h.get('desired') != prev:
            changes.append(dict(t=rel(s['ts']), desired=h.get('desired'), current=h.get('current'), cpu=h.get('cpu')))
            prev = h.get('desired')
    out['hpa_desired_changes'] = changes
    pods = {}
    for s in tl:
        for p in s['pods']:
            i = pods.setdefault(p['name'], dict(created=rel(k8s_ts(p['created']))))
            if p['ready'] and 'ready' not in i:
                i['ready'] = rel(s['ts'])
            if p['deleting'] and 'deleting' not in i:
                i['deleting'] = rel(s['ts'])
        names = {p['name'] for p in s['pods']}
        for n, i in pods.items():
            if n not in names and 'gone' not in i:
                i['gone'] = rel(s['ts'])
    out['pods'] = pods
    out['alerts'] = [dict(t=rel(e['ts']), alert=e['alert'], state=e['state']) for e in ev if e['event'] == 'alert_state']
    cpu = [(rel(s['ts']), (s.get('hpa') or {}).get('cpu')) for s in tl]
    out['cpu_max'] = max((c for _, c in cpu if c is not None), default=None)
    out['share_by_pod'] = {}
    for n in pods:
        out['share_by_pod'][n[-5:]] = sum(1 for r in recs if r.get('pod') == n)
    return out


if __name__ == '__main__':
    res = {}
    c = ROOT / 'results/expC'
    res['scale'] = stats(c, '0–30 с: 1 запр/с; 30–270 с: 4,5 запр/с; 270–570 с: 0,5 запр/с')
    if (c / 'hot' / 'timeline.jsonl').exists() and list((c / 'hot').glob('requests-*.jsonl')):
        res['hot'] = stats(c / 'hot', '0–200 с: 2,5 запр/с, skew80 через ClientIP')
    json.dump(res, open(ROOT / 'results/tables/expC.json', 'w'), ensure_ascii=False, indent=1)
    print(json.dumps(res, ensure_ascii=False, indent=1))
