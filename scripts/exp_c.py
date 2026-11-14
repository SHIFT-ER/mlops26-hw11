"""Опыт C: HPA autoscaling/v2 (min 2, max 3, CPU) и два алерта Prometheus.

  python scripts/exp_c.py scale   # равномерная нагрузка через llm-sim: 1 -> 4.5 -> 0.5 запр/с, 2→3→2
  python scripts/exp_c.py hot     # 80% через клиента 0 по ClientIP при включённом HPA
Параллельно: наблюдатель (Pod/EndpointSlice/HPA каждые 0.5 с) и опрос /api/v1/alerts Prometheus.
"""
import json
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from kexp import ROOT, Watcher, finish_load, helm_upgrade, kubectl, log_event, read_jsonl, run_load, sh, summarize  # noqa: E402

PROM = 'http://127.0.0.1:19090'


class AlertPoller(threading.Thread):
    def __init__(self, path):
        super().__init__(daemon=True)
        self.path, self.stop_ev, self.last = path, threading.Event(), {}

    def run(self):
        while not self.stop_ev.is_set():
            try:
                data = json.load(urllib.request.urlopen(PROM + '/api/v1/alerts', timeout=3))['data']['alerts']
                cur = {a['labels']['alertname']: a['state'] for a in data}
                for name in set(cur) | set(self.last):
                    if cur.get(name, 'inactive') != self.last.get(name, 'inactive'):
                        log_event(self.path, event='alert_state', alert=name,
                                  state=cur.get(name, 'inactive'), prev=self.last.get(name, 'inactive'))
                self.last = cur
            except Exception as exc:  # noqa: BLE001
                print('alerts poll', exc)
            self.stop_ev.wait(2)


def port_forward():
    return subprocess.Popen(['kubectl', '-n', 'monitoring', 'port-forward', 'svc/prometheus', '19090:9090'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def prom_range(query, start, end, step=5):
    from urllib.parse import urlencode
    url = PROM + '/api/v1/query_range?' + urlencode(dict(query=query, start=start, end=end, step=step))
    return json.load(urllib.request.urlopen(url, timeout=10))['data']['result']


def wait_replicas(n, timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        out = kubectl('get', 'deploy/llm-sim', '-o', 'jsonpath={.status.readyReplicas}/{.spec.replicas}')
        if out == f'{n}/{n}':
            return
        time.sleep(3)


def experiment(name, profile, traffic, target, settle):
    out = ROOT / f'results/expC/{name}' if name != 'scale' else ROOT / 'results/expC'
    out.mkdir(parents=True, exist_ok=True)
    ev = out / 'events.jsonl'
    pf = port_forward()
    time.sleep(2)
    helm_upgrade('values-hpa.yaml')
    wait_replicas(2)
    log_event(ev, event='hpa_config', hpa=json.loads(kubectl('get', 'hpa', 'llm-sim', '-o', 'json'))['spec'])
    sched = out / 'schedule.json'
    sh(sys.executable, '-m', 'loadgen.schedule', '--profile', profile, '--warmup', '0', '--traffic', traffic,
       '--chunks', '16,32', '--seed', '31', '--out', str(sched))
    poller = AlertPoller(ev)
    poller.start()
    with Watcher(out, hpa=True):
        t_start = time.time()
        procs, meta = run_load(sched, out, f'C-{name}', target, wait=False, timeout=30)
        f = finish_load(procs, meta, out)
        log_event(ev, event='load_finished')
        # ждём возврата к минимуму (окно стабилизации scaleDown 60 с + период HPA 15 с)
        t_end_load = time.time()
        while time.time() - t_end_load < settle:
            r = kubectl('get', 'hpa', 'llm-sim', '-o', 'jsonpath={.status.desiredReplicas}')
            if r == '2' and kubectl('get', 'deploy/llm-sim', '-o', 'jsonpath={.status.replicas}') == '2':
                log_event(ev, event='back_to_min', after_load_s=round(time.time() - t_end_load, 1))
                break
            time.sleep(5)
        time.sleep(20)
    poller.stop_ev.set()
    t_stop = time.time()
    (out / 'hpa-describe.txt').write_text(kubectl('describe', 'hpa', 'llm-sim'))
    (out / 'hpa.yaml').write_text(kubectl('get', 'hpa', 'llm-sim', '-o', 'yaml'))
    (out / 'k8s-events.txt').write_text(kubectl('get', 'events', '--sort-by=.lastTimestamp'))
    queries = {
        'alerts': 'ALERTS',
        'ttfc_p95': 'histogram_quantile(0.95, sum by (le) (rate(sim_ttfc_seconds_bucket[1m])))',
        'error_ratio': 'sum(rate(sim_requests_total{outcome!="ok"}[1m])) / sum(rate(sim_requests_total[1m]))',
        'requests_1m': 'sum(increase(sim_requests_total[1m]))',
        'cpu_by_pod': 'sum by (pod) (rate(container_cpu_usage_seconds_total{container="sim"}[1m]))',
        'running_by_pod': 'sum by (pod) (sim_running)', 'waiting_by_pod': 'sum by (pod) (sim_waiting)',
        'ready_replicas': 'kube_deployment_status_replicas_ready{deployment="llm-sim"}',
        'hpa_desired': 'kube_horizontalpodautoscaler_status_desired_replicas',
        'ok_rps_by_pod': 'sum by (pod) (rate(sim_requests_total{outcome="ok"}[1m]))',
    }
    prom = {k: prom_range(q, t_start - 30, t_stop) for k, q in queries.items()}
    json.dump(dict(queries=queries, start=t_start - 30, end=t_stop, step=5, result=prom),
              open(out / 'prometheus-range.json', 'w'))
    pf.terminate()
    s = summarize(read_jsonl(f))
    log_event(ev, event='summary', **s)


if __name__ == '__main__':
    if sys.argv[1] == 'scale':
        experiment('scale', '30:1,240:4.5,300:0.5', 'uniform', 'http://llm-sim:8000', settle=420)
    else:
        experiment('hot', '200:2.5', 'skew80', 'http://llm-sim-clientip:8000', settle=300)
