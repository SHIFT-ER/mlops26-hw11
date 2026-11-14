"""Опыт A: прогрев, rolling update, неготовая версия и откат, принудительное завершение процесса.

  python scripts/exp_a.py warm-naive   # исходный дефект: startup/readiness = /health
  python scripts/exp_a.py warm-fixed   # исправление: /startup и /ready ждут конца прогрева
  python scripts/exp_a.py rollout      # A -> B под нагрузкой с длительными потоками
  python scripts/exp_a.py broken       # заведомо неготовая B, затем helm rollback к A
  python scripts/exp_a.py kill         # kill -9 процесса сервиса под нагрузкой, повторы клиента
Все этапы одной нагрузки: Service llm-sim (обычный), 4 клиентских Pod, соединение на каждый запрос
(--no-keepalive): с keep-alive kube-proxy выбирает Pod только для новых соединений, и дефект проявляется
слабее и позже (5 вместо 14 запросов), см. results/expA/warm-naive-keepalive.
"""
import json
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from kexp import (ROOT, Watcher, finish_load, helm_upgrade, kjson, kubectl, log_event,  # noqa: E402
                  read_jsonl, run_load, sh, summarize)

TARGET = 'http://llm-sim:8000'


def schedule(out: Path, duration, rate, longs=(), chunks='8,16', seed=7):
    args = [sys.executable, '-m', 'loadgen.schedule', '--profile', f'{duration}:{rate}', '--warmup', '0',
            '--traffic', 'uniform', '--chunks', chunks, '--seed', str(seed), '--out', str(out)]
    for l in longs:
        args += ['--long', l]
    sh(*args)
    return out


def backend_pods():
    items = kjson('pods', '-l', 'app.kubernetes.io/component=backend')['items']
    return [p['metadata']['name'] for p in items if not p['metadata'].get('deletionTimestamp')]


def wait_all_warm(n=2, timeout=180):
    """Дождаться n Pod, у которых /startup = 200 (проверка напрямую, а не по Ready)."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        items = kjson('pods', '-l', 'app.kubernetes.io/component=backend')['items']
        live = [p for p in items if not p['metadata'].get('deletionTimestamp') and p['status'].get('podIP')]
        warm = 0
        for p in live:
            r = kubectl('exec', 'loadclient-3', '--', 'curl', '-s', '-o', '/dev/null', '-w', '%{http_code}',
                        f'http://{p["status"]["podIP"]}:8000/startup', check=False)
            warm += r.strip() == '200'
        if len(live) == n and warm == n:
            return
        time.sleep(2)
    raise TimeoutError('pods not warm')


def at(delay, fn):
    t = threading.Timer(delay, fn)
    t.start()
    return t


def warm(mode):
    out = ROOT / f'results/expA/warm-{mode}'
    out.mkdir(parents=True, exist_ok=True)
    ev = out / 'events.jsonl'
    values = ['values-naive-probes.yaml'] if mode == 'naive' else []
    helm_upgrade(*values)
    kubectl('rollout', 'status', 'deploy/llm-sim', '--timeout=180s')
    wait_all_warm()
    log_event(ev, event='baseline_warm', pods=backend_pods(), probes=mode)
    sched = schedule(out / 'schedule.json', 100, 1.2)
    with Watcher(out):
        procs, meta = run_load(sched, out, f'A-warm-{mode}', TARGET, wait=False, keepalive=False)
        victim = backend_pods()[0]

        def kill():
            log_event(ev, event='delete_pod', pod=victim)
            kubectl('delete', 'pod', victim, '--wait=false')
        at(25, kill)
        f = finish_load(procs, meta, out)
    recs = read_jsonl(f)
    s = summarize(recs)
    s['served_cold'] = sum(1 for r in recs if r.get('warm_at_receive') is False)
    log_event(ev, event='summary', **s)


def rollout():
    out = ROOT / 'results/expA/rollout'
    out.mkdir(parents=True, exist_ok=True)
    ev = out / 'events.jsonl'
    helm_upgrade()
    kubectl('rollout', 'status', 'deploy/llm-sim', '--timeout=180s')
    wait_all_warm()
    # два длительных потока (~45 и ~70 с) стартуют до начала обновления и переживают удаление старых Pod
    sched = schedule(out / 'schedule.json', 150, 1.2, longs=['5:2800:1', '8:4200:2'])
    with Watcher(out):
        procs, meta = run_load(sched, out, 'A-rollout', TARGET, wait=False, timeout=90, keepalive=False)

        def upgrade():
            log_event(ev, event='helm_upgrade_start', to='B')
            helm_upgrade('values-B.yaml')
            log_event(ev, event='helm_upgrade_applied', to='B')
            r = kubectl('rollout', 'status', 'deploy/llm-sim', '--timeout=240s', check=False)
            log_event(ev, event='rollout_status_done', out=r.strip()[-200:])
        at(4 + 15, upgrade)
        f = finish_load(procs, meta, out)
    s = summarize(read_jsonl(f))
    log_event(ev, event='summary', **s)


def broken():
    out = ROOT / 'results/expA/broken'
    out.mkdir(parents=True, exist_ok=True)
    ev = out / 'events.jsonl'
    helm_upgrade()  # исходно A
    kubectl('rollout', 'status', 'deploy/llm-sim', '--timeout=240s')
    wait_all_warm()
    hist = json.loads(sh('helm', 'history', 'sim', '-n', 'llm', '-o', 'json'))
    rev_a = hist[-1]['revision']
    log_event(ev, event='baseline', revision=rev_a, version='A')
    sched = schedule(out / 'schedule.json', 150, 1.2)
    with Watcher(out):
        procs, meta = run_load(sched, out, 'A-broken', TARGET, wait=False, keepalive=False)

        def release_broken():
            log_event(ev, event='helm_upgrade_start', to='B-broken')
            helm_upgrade('values-B-broken.yaml')
            log_event(ev, event='helm_upgrade_applied', to='B-broken')

        def rollback():
            log_event(ev, event='helm_rollback_start', to_revision=rev_a)
            sh('helm', 'rollback', 'sim', str(rev_a), '-n', 'llm')
            log_event(ev, event='helm_rollback_applied', to_revision=rev_a)
            r = kubectl('rollout', 'status', 'deploy/llm-sim', '--timeout=120s', check=False)
            log_event(ev, event='rollout_status_done', out=r.strip()[-200:])
        at(4 + 15, release_broken)
        at(4 + 85, rollback)
        f = finish_load(procs, meta, out)
    recs = read_jsonl(f)
    s = summarize(recs)
    s['versions'] = sorted({r.get('model_version') for r in recs if r.get('model_version')})
    log_event(ev, event='summary', **s)
    log_event(ev, event='history', history=json.loads(sh('helm', 'history', 'sim', '-n', 'llm', '-o', 'json')))


def kill():
    out = ROOT / 'results/expA/kill'
    out.mkdir(parents=True, exist_ok=True)
    ev = out / 'events.jsonl'
    wait_all_warm()
    sched = schedule(out / 'schedule.json', 100, 1.2, longs=['20:600:1', '22:600:2', '24:600:3'], chunks='16,32')
    with Watcher(out):
        procs, meta = run_load(sched, out, 'A-kill', TARGET, wait=False, retries=1, keepalive=False)
        victim = backend_pods()[0]

        def do_kill():
            log_event(ev, event='kill9', pod=victim)
            kubectl('exec', victim, '--', 'sh', '-c', 'kill -9 $(cat /tmp/app.pid)', check=False)
        at(4 + 27, do_kill)
        f = finish_load(procs, meta, out)
    s = summarize(read_jsonl(f))
    log_event(ev, event='summary', **s)


if __name__ == '__main__':
    dict(**{'warm-naive': lambda: warm('naive'), 'warm-fixed': lambda: warm('fixed'),
            'rollout': rollout, 'broken': broken, 'kill': kill})[sys.argv[1]]()
