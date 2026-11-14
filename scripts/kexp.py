"""Общая обвязка опытов на хосте: kubectl/helm, наблюдатель Ready/EndpointSlice/HPA, логи Pod, нагрузка.

Всё сырое пишется в results/<опыт>/:
  timeline.jsonl   — каждые 0.5 с: Pod (phase, ready, версия), Ready-адреса EndpointSlice, HPA;
  server-logs/     — журналы каждого Pod сервиса (kubectl logs -f, подхватываются новые Pod);
  requests-*.jsonl — сырые попытки клиентов (loadgen.client).
"""
import json
import subprocess
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NS = 'llm'
CLIENTS = [f'loadclient-{i}' for i in range(4)]


def sh(*cmd, check=True, capture=True, input=None, timeout=None):
    r = subprocess.run(cmd, check=False, text=True, capture_output=capture, input=input, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f'{" ".join(cmd)}: {r.stderr.strip()}')
    return r.stdout


def kubectl(*args, **kw):
    return sh('kubectl', '-n', NS, *args, **kw)


def kjson(*args):
    return json.loads(kubectl('get', *args, '-o', 'json'))


def helm_upgrade(*values, sets=(), wait=False, timeout='5m'):
    # --reset-values: без него helm upgrade без -f/--set переиспользует значения прошлого релиза
    cmd = ['helm', 'upgrade', '--install', 'sim', str(ROOT / 'charts/llm-sim'), '-n', NS, '--reset-values']
    for v in values:
        cmd += ['-f', str(ROOT / 'charts/llm-sim' / v)]
    for s in sets:
        cmd += ['--set', s]
    if wait:
        cmd += ['--wait', '--timeout', timeout]
    return sh(*cmd)


def log_event(path, **ev):
    ev.setdefault('ts', time.time())
    with open(path, 'a') as f:
        f.write(json.dumps(ev, ensure_ascii=False) + '\n')
    print(time.strftime('%H:%M:%S'), json.dumps(ev, ensure_ascii=False)[:200], flush=True)


class Watcher:
    """Фоновый опрос состояния кластера + kubectl logs -f для каждого нового Pod сервиса."""

    def __init__(self, outdir: Path, period=0.5, hpa=False):
        self.out = Path(outdir)
        (self.out / 'server-logs').mkdir(parents=True, exist_ok=True)
        self.period, self.hpa = period, hpa
        self.stop_ev = threading.Event()
        self.followers = {}
        self.thread = threading.Thread(target=self.run, daemon=True)

    def snapshot(self):
        pods = kjson('pods', '-l', 'app.kubernetes.io/component=backend')['items']
        slices = kjson('endpointslices', '-l', 'kubernetes.io/service-name=llm-sim')['items']
        snap = dict(ts=time.time(), pods=[], endpoints_ready=[], endpoints_serving=[])
        for p in pods:
            conds = {c['type']: c['status'] for c in p['status'].get('conditions', [])}
            cs = (p['status'].get('containerStatuses') or [{}])[0]
            snap['pods'].append(dict(
                name=p['metadata']['name'], phase=p['status'].get('phase'), ip=p['status'].get('podIP'),
                version=p['metadata']['labels'].get('model-version'), ready=conds.get('Ready') == 'True',
                started=cs.get('started'), restarts=cs.get('restartCount', 0),
                deleting=p['metadata'].get('deletionTimestamp') is not None,
                created=p['metadata']['creationTimestamp']))
        for sl in slices:
            for ep in sl.get('endpoints') or []:
                name = (ep.get('targetRef') or {}).get('name')
                if ep['conditions'].get('ready'):
                    snap['endpoints_ready'].append(name)
                if ep['conditions'].get('serving'):
                    snap['endpoints_serving'].append(name)
        if self.hpa:
            try:
                h = kjson('hpa', 'llm-sim')
                st = h.get('status', {})
                cpu = next((m['resource']['current'].get('averageUtilization') for m in st.get('currentMetrics') or []
                            if m.get('type') == 'Resource'), None)
                snap['hpa'] = dict(current=st.get('currentReplicas'), desired=st.get('desiredReplicas'), cpu=cpu)
            except Exception:  # noqa: BLE001
                snap['hpa'] = None
        return snap, [p['metadata']['name'] for p in pods]

    def follow(self, pod):
        f = open(self.out / 'server-logs' / f'{pod}.jsonl', 'a')
        self.followers[pod] = subprocess.Popen(['kubectl', '-n', NS, 'logs', '-f', pod, '--all-containers'],
                                               stdout=f, stderr=subprocess.DEVNULL)

    def run(self):
        with open(self.out / 'timeline.jsonl', 'a') as tl:
            while not self.stop_ev.is_set():
                try:
                    snap, names = self.snapshot()
                    tl.write(json.dumps(snap) + '\n')
                    tl.flush()
                    started = {p['name'] for p in snap['pods'] if p['started'] and not p['deleting']}
                    for n in names:
                        if n not in self.followers:
                            self.follow(n)
                        elif self.followers[n].poll() is not None and n in started:
                            self.follow(n)  # контейнер перезапущен: дописываем журнал нового контейнера
                except Exception as exc:  # noqa: BLE001
                    print('watch error', exc, flush=True)
                self.stop_ev.wait(self.period)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *a):
        time.sleep(2)
        self.stop_ev.set()
        self.thread.join()
        for p in self.followers.values():
            p.terminate()


def push_schedule(path: Path, clients=CLIENTS):
    data = Path(path).read_text()
    for c in clients:
        kubectl('exec', '-i', c, '--', 'sh', '-c', 'cat > /tmp/schedule.json', input=data)


def run_load(schedule: Path, outdir: Path, run: str, target: str, clients=range(4), retries=0, timeout=30,
             start_delay=4.0, wait=True, keepalive=True):
    """Запустить клиентов во всех клиентских Pod с общим start_at; вернуть путь к объединённому JSONL."""
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    push_schedule(schedule, [CLIENTS[i] for i in clients])
    start_at = time.time() + start_delay
    procs = []
    for i in clients:
        cmd = ['kubectl', '-n', NS, 'exec', CLIENTS[i], '--', 'python', '-m', 'loadgen.client',
               '--schedule', '/tmp/schedule.json', '--client', str(i), '--target', target,
               '--start-at', f'{start_at:.3f}', '--run', run, '--retries', str(retries),
               '--timeout', str(timeout), '--out', f'/tmp/{run}.jsonl'] + ([] if keepalive else ['--no-keepalive'])
        procs.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
    sched_rel = str(Path(schedule).resolve().relative_to(ROOT)) if Path(schedule).resolve().is_relative_to(ROOT) else str(schedule)
    meta = dict(run=run, target=target, start_at=start_at, schedule=sched_rel, retries=retries, timeout=timeout,
                keepalive=keepalive)
    if not wait:
        return procs, meta

    return finish_load(procs, meta, outdir, clients)


def finish_load(procs, meta, outdir, clients=range(4)):
    for p in procs:
        out, err = p.communicate()
        if p.returncode:
            print('client failed', err[-500:], flush=True)
    merged = Path(outdir) / f'requests-{meta["run"]}.jsonl'
    with open(merged, 'w') as f:
        for i in clients:
            f.write(kubectl('exec', CLIENTS[i], '--', 'cat', f'/tmp/{meta["run"]}.jsonl'))
    with open(Path(outdir) / 'runs.jsonl', 'a') as f:
        f.write(json.dumps(meta) + '\n')
    return merged


def read_jsonl(path):
    out = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line.startswith('{'):
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def summarize(recs, phase=None):
    """Учёт попыток: запланированные/отправленные/завершённые/отклонённые/оборванные/просроченные."""
    import statistics
    rs = [r for r in recs if phase is None or r['phase'] == phase]
    by = {}
    for r in rs:
        by[r['outcome']] = by.get(r['outcome'], 0) + 1
    ok = [r for r in rs if r['outcome'] == 'ok']

    def pct(vals, q):
        if not vals:
            return None
        vals = sorted(vals)
        k = max(0, min(len(vals) - 1, int(round(q * len(vals) + 0.5)) - 1))  # nearest-rank
        return round(vals[k], 4)
    ttfc = [r['ttfc_s'] for r in ok]
    dur = [r['duration_s'] for r in ok]
    logical = {r['id'] for r in rs}
    final_ok = {r['id'] for r in ok}
    return dict(attempts=len(rs), logical=len(logical), outcomes=by, ok=len(ok), logical_ok=len(final_ok),
                error_share=round(1 - len(ok) / len(rs), 4) if rs else None,
                timeout_share=round(by.get('timeout', 0) / len(rs), 4) if rs else None,
                ttfc_p50=pct(ttfc, .5), ttfc_p95=pct(ttfc, .95), dur_p50=pct(dur, .5), dur_p95=pct(dur, .95),
                n_ttfc=len(ttfc), send_lag_p95=pct([r['send_lag_s'] for r in rs], .95),
                send_lag_max=max((r['send_lag_s'] for r in rs), default=None),
                mean_ttfc=round(statistics.mean(ttfc), 4) if ttfc else None)
