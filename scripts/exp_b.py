"""Опыт B: обычный Service vs sessionAffinity: ClientIP vs cache-aware маршрутизатор.

Для каждого повтора (seed) и вида трафика (uniform, skew80) все три политики получают ОДНО и
то же расписание (запросы, длины входов, max_chunks, моменты отправки). Перед каждым прогоном
локальные кеши обеих реплик очищаются (явное начальное состояние); первые 15 с — фаза warmup,
затем 120 с измерения. Порядок политик внутри повтора меняется по кругу.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from kexp import ROOT, helm_upgrade, kjson, kubectl, read_jsonl, run_load, sh, summarize  # noqa: E402

OUT = ROOT / 'results/expB'
TARGETS = {'default': 'http://llm-sim:8000', 'clientip': 'http://llm-sim-clientip:8000',
           'router': 'http://llm-router:8000'}
RATE, WARMUP, MEASURE = 2.5, 30, 120


def reset_caches():
    pods = kjson('pods', '-l', 'app.kubernetes.io/component=backend')['items']
    for p in pods:
        ip = p['status']['podIP']
        kubectl('exec', 'loadclient-3', '--', 'curl', '-s', '-X', 'POST', f'http://{ip}:8000/admin/reset-cache')
    return [p['metadata']['name'] for p in pods]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    helm_upgrade(wait=True)  # базовая конфигурация чарта: версия A, исправленные probes, 2 реплики, без HPA
    kubectl('rollout', 'status', 'deploy/llm-sim', '--timeout=240s')
    (OUT / 'helm-values.yaml').write_text(sh('helm', 'get', 'values', 'sim', '-n', 'llm', '--all'))
    (OUT / 'services.yaml').write_text(kubectl('get', 'svc', '-o', 'yaml'))
    time.sleep(25)  # прогрев
    reps = [int(x) for x in (sys.argv[1:] or ['1', '2', '3'])]
    order = list(TARGETS)
    for rep in reps:
        for traffic in ('uniform', 'skew80'):
            sched = OUT / f'schedule-{traffic}-r{rep}.json'
            sh(sys.executable, '-m', 'loadgen.schedule', '--profile', f'{WARMUP}:{RATE},{MEASURE}:{RATE}',
               '--warmup', str(WARMUP), '--traffic', traffic, '--seed', str(100 + rep), '--out', str(sched))
            rot = order[rep % 3:] + order[:rep % 3]
            for policy in rot:
                pods = reset_caches()
                run = f'B-{policy}-{traffic}-r{rep}'
                t0 = time.time()
                f = run_load(sched, OUT, run, TARGETS[policy], timeout=30)
                s = summarize(read_jsonl(f), 'measure')
                with open(OUT / 'summary.jsonl', 'a') as fh:
                    fh.write(json.dumps(dict(run=run, policy=policy, traffic=traffic, rep=rep, pods=pods,
                                             started=t0, finished=time.time(), **s)) + '\n')
                print(run, json.dumps(s), flush=True)
                time.sleep(5)


if __name__ == '__main__':
    main()
