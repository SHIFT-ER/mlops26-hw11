"""Сводка опыта A по сырым данным: события, линия Ready/EndpointSlice, журналы сервера и клиентов.

  uv run python analysis/expa_stats.py  ->  results/tables/expA.json
"""
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from kexp import read_jsonl, summarize  # noqa: E402

A = ROOT / 'results/expA'


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%H:%M:%S.%f')[:-3] + 'Z'


def k8s_ts(s):
    return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp()


def load(part):
    d = A / part
    recs = []
    for f in d.glob('requests-*.jsonl'):
        recs += read_jsonl(f)
    server = {}
    for f in (d / 'server-logs').glob('*.jsonl'):
        server[f.stem] = read_jsonl(f)
    return d, recs, read_jsonl(d / 'timeline.jsonl'), read_jsonl(d / 'events.jsonl'), server


def pod_lines(timeline):
    """Для каждого Pod: первый момент появления, started, Ready, в EndpointSlice, удаление, исчезновение."""
    info = {}
    for s in timeline:
        names = set()
        for p in s['pods']:
            n = p['name']
            names.add(n)
            i = info.setdefault(n, dict(version=p['version'], first_seen=s['ts'], created=k8s_ts(p['created'])))
            if p['started'] and 'started' not in i:
                i['started'] = s['ts']
            if p['ready'] and 'ready' not in i:
                i['ready'] = s['ts']
            if n in s['endpoints_ready'] and 'in_endpoints' not in i:
                i['in_endpoints'] = s['ts']
            if p['deleting'] and 'deleting' not in i:
                i['deleting'] = s['ts']
            if i.get('deleting') and n not in s['endpoints_ready'] and 'out_of_endpoints' not in i:
                i['out_of_endpoints'] = s['ts']
            i['restarts'] = p['restarts']
            if not p['ready'] and i.get('ready') and 'not_ready_after' not in i and not p['deleting']:
                i['not_ready_after'] = s['ts']
            if i.get('not_ready_after') and p['ready'] and s['ts'] > i['not_ready_after'] and 'ready_again' not in i:
                i['ready_again'] = s['ts']
        for n, i in info.items():
            if n not in names and 'gone' not in i and s['ts'] > i['first_seen']:
                i['gone'] = s['ts']
    return info


def warm(part):
    d, recs, tl, ev, server = load(part)
    pods = pod_lines(tl)
    t_del = next(e['ts'] for e in ev if e['event'] == 'delete_pod')
    new = [n for n, i in pods.items() if i['created'] >= t_del - 1]
    out = dict(part=part, summary=summarize(recs), deleted=next(e['pod'] for e in ev if e['event'] == 'delete_pod'))
    for n in new:
        i = pods[n]
        lines = server.get(n, [])
        wd = next((x['ts'] for x in lines if x.get('event') == 'warmup_done'), None)
        ws = next((x['ts'] for x in lines if x.get('event') == 'warmup_start'), None)
        reqs = [x for x in lines if x.get('event') == 'request']
        cold = [x for x in reqs if x.get('warm_at_receive') is False]
        first_req = min((x['ts_received'] for x in reqs), default=None)
        out['new_pod'] = dict(
            name=n, created_s=round(i['created'] - t_del, 1), warmup_start_s=round(ws - t_del, 1) if ws else None,
            ready_s=round(i['ready'] - t_del, 1) if 'ready' in i else None,
            in_endpoints_s=round(i['in_endpoints'] - t_del, 1) if 'in_endpoints' in i else None,
            warmup_done_s=round(wd - t_del, 1) if wd else None,
            first_request_s=round(first_req - t_del, 1) if first_req else None,
            requests_total=len(reqs), requests_received_cold=len(cold),
            cold_ttfc_max=max((x['ttfc_s'] for x in cold if x.get('ttfc_s')), default=None),
            cold_ttfc_min=min((x['ttfc_s'] for x in cold if x.get('ttfc_s')), default=None))
    client_cold = [r for r in recs if r.get('warm_at_receive') is False]
    out['client_cold'] = dict(n=len(client_cold), ttfc_max=max((r['ttfc_s'] for r in client_cold if r['ttfc_s']), default=None),
                              outcomes=dict(Counter(r['outcome'] for r in client_cold)))
    ok = [r for r in recs if r['outcome'] == 'ok']
    out['ttfc_p95_all'] = summarize(recs)['ttfc_p95']
    out['ttfc_max_all'] = max(r['ttfc_s'] for r in ok)
    return out


def rollout():
    d, recs, tl, ev, server = load('rollout')
    pods = pod_lines(tl)
    t_up = next(e['ts'] for e in ev if e['event'] == 'helm_upgrade_start')
    rel = lambda t: None if t is None else round(t - t_up, 1)  # noqa: E731
    rows = []
    for n, i in sorted(pods.items(), key=lambda x: x[1]['created']):
        lines = server.get(n, [])
        drain = next((x for x in lines if x.get('event') == 'drain_start'), None)
        stop = next((x for x in lines if x.get('event') == 'process_stop'), None)
        rows.append(dict(pod=n, version=i['version'], created=rel(i['created']), ready=rel(i.get('ready')),
                         in_endpoints=rel(i.get('in_endpoints')), deleting=rel(i.get('deleting')),
                         drain_start=rel(drain['ts']) if drain else None,
                         running_at_drain=drain['running'] if drain else None,
                         out_of_endpoints=rel(i.get('out_of_endpoints')),
                         process_stop=rel(stop['ts']) if stop else None, gone=rel(i.get('gone')),
                         requests=sum(1 for x in lines if x.get('event') == 'request')))
    longs = [dict(id=r['id'], pod=r.get('pod'), version=r.get('model_version'), start=rel(r['send_wall']),
                  end=rel(r['done_wall']), chunks=r['chunks'], outcome=r['outcome']) for r in recs if r.get('long')]
    by_v = Counter((r.get('model_version'), r['outcome']) for r in recs)
    s = summarize(recs)
    return dict(part='rollout', summary=s, pods=rows, long_streams=longs,
                by_version_outcome={f'{k[0]}:{k[1]}': v for k, v in by_v.items()},
                http_5xx=s['outcomes'].get('http_5xx', 0), aborted=s['outcomes'].get('aborted', 0),
                events=[dict(event=e['event'], t=rel(e['ts'])) for e in ev if e['event'] != 'summary'])


def broken():
    d, recs, tl, ev, server = load('broken')
    pods = pod_lines(tl)
    t_up = next(e['ts'] for e in ev if e['event'] == 'helm_upgrade_start')
    t_rb = next(e['ts'] for e in ev if e['event'] == 'helm_rollback_start')
    rel = lambda t: None if t is None else round(t - t_up, 1)  # noqa: E731
    bpods = [n for n, i in pods.items() if i['version'] == 'B' and i['created'] >= t_up - 1]
    fails = {n: [x.get('error') for x in server.get(n, []) if x.get('event') == 'load_failed'][:1] for n in bpods}
    after = [r for r in recs if r['send_wall'] > t_rb + 5]
    before = [r for r in recs if t_up < r['send_wall'] < t_rb]
    hist = next((e['history'] for e in ev if e['event'] == 'history'), [])
    return dict(part='broken', summary=summarize(recs), rollback_at=rel(t_rb),
                b_pods=[dict(pod=n, ready=pods[n].get('ready') is not None, in_endpoints=pods[n].get('in_endpoints') is not None,
                             restarts=pods[n]['restarts'], gone=rel(pods[n].get('gone')), error=fails[n]) for n in bpods],
                versions_between=dict(Counter(r.get('model_version') for r in before)),
                versions_after_rollback=dict(Counter(r.get('model_version') for r in after)),
                sha_after_rollback=sorted({r.get('model_sha256') for r in after if r.get('model_sha256')}),
                helm_history=[dict(revision=h['revision'], status=h['status'], description=h.get('description'))
                              for h in hist][-4:])


def kill():
    d, recs, tl, ev, server = load('kill')
    pods = pod_lines(tl)
    k = next(e for e in ev if e['event'] == 'kill9')
    t_k = k['ts']
    i = pods[k['pod']]
    # обнаружение: первый снимок после kill, где Pod не Ready / не в EndpointSlice
    det = next((s['ts'] for s in tl if s['ts'] > t_k and k['pod'] not in s['endpoints_ready']), None)
    back = next((s['ts'] for s in tl if det and s['ts'] > det and k['pod'] in s['endpoints_ready']), None)
    rel = lambda t: None if t is None else round(t - t_k, 1)  # noqa: E731
    first = [r for r in recs if r['attempt'] == 0]
    retries = [r for r in recs if r['attempt'] > 0]
    broken_first = [r for r in first if r['outcome'] != 'ok']
    return dict(part='kill', pod=k['pod'], summary=summarize(recs),
                restarts=i['restarts'], detected_not_ready_s=rel(det), ready_again_s=rel(back),
                failed_first_attempts=dict(Counter(r['outcome'] for r in broken_first)),
                failed_first_on_victim=sum(1 for r in broken_first if r.get('pod') == k['pod']),
                failed_errors=dict(Counter((r.get('error') or '')[:60] for r in broken_first)),
                retries=len(retries), retries_ok=sum(r['outcome'] == 'ok' for r in retries),
                logical=len({r['id'] for r in recs}), logical_ok=len({r['id'] for r in recs if r['outcome'] == 'ok'}),
                long_streams=[dict(id=r['id'], attempt=r['attempt'], pod=r.get('pod'), outcome=r['outcome'],
                                   chunks=r['chunks'], of=r['max_chunks']) for r in recs if r.get('long')],
                window_failures=[dict(t=rel(r['send_wall']), outcome=r['outcome'], pod=r.get('pod'), attempt=r['attempt'])
                                 for r in recs if r['outcome'] != 'ok'][:40])


def keepalive_note():
    d = A / 'warm-naive-keepalive'
    if not d.exists():
        return None
    tl = read_jsonl(d / 'timeline.jsonl')
    pods = pod_lines(tl)
    server = {f.stem: read_jsonl(f) for f in (d / 'server-logs').glob('*.jsonl')}
    ev = read_jsonl(d / 'events.jsonl')
    t_del = next(e['ts'] for e in ev if e['event'] == 'delete_pod')
    new = [n for n, i in pods.items() if i['created'] >= t_del - 1]
    return [dict(pod=n, ready_s=round(pods[n]['ready'] - t_del, 1), in_endpoints_s=round(pods[n]['in_endpoints'] - t_del, 1),
                 requests=sum(1 for x in server.get(n, []) if x.get('event') == 'request'),
                 observed_until_s=round(tl[-1]['ts'] - t_del, 1)) for n in new]


if __name__ == '__main__':
    res = {}
    for name, fn in [('warm-naive', lambda: warm('warm-naive')), ('warm-fixed', lambda: warm('warm-fixed')),
                     ('rollout', rollout), ('broken', broken), ('kill', kill), ('keepalive', keepalive_note)]:
        try:
            res[name] = fn()
        except (FileNotFoundError, StopIteration, ValueError) as exc:
            res[name] = dict(error=repr(exc))
    (ROOT / 'results/tables').mkdir(parents=True, exist_ok=True)
    json.dump(res, open(ROOT / 'results/tables/expA.json', 'w'), ensure_ascii=False, indent=1)
    print(json.dumps(res, ensure_ascii=False, indent=1)[:6000])
