"""Сверка перцентилей TTFC: classic histogram в Prometheus vs журнал сервера vs клиентский журнал.

Для каждого выбранного прогона опыта B окно = [первая отправка - 2 с, последнее завершение + 6 с].
PromQL берёт суммарные bucket-счётчики всех Pod за это окно (сохраняя le):
  histogram_quantile(q, sum by (le) (increase(sim_ttfc_seconds_bucket[<окно>s] @ <конец>)))
Набор ответов один и тот же (прогон отделён паузой); число наблюдений сравнивается явно.
Нужен port-forward Prometheus на 127.0.0.1:19090.
"""
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from kexp import read_jsonl  # noqa: E402

PROM = 'http://127.0.0.1:19090'
LE = [.01, .025, .05, .1, .2, .3, .5, .75, 1, 1.5, 2, 3, 4, 6, 8, 12, 20, 30, 45, 60]


def q(expr, at):
    url = PROM + '/api/v1/query?' + urlencode(dict(query=expr, time=at))
    res = json.load(urllib.request.urlopen(url, timeout=10))['data']['result']
    return float(res[0]['value'][1]) if res else None


def nearest_rank(vals, p):
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, int(round(p * len(vals) + 0.5)) - 1))
    return vals[k]


def bucket_of(v):
    for b in LE:
        if v <= b:
            return b
    return float('inf')


def main(runs):
    pf = subprocess.Popen(['kubectl', '-n', 'monitoring', 'port-forward', 'svc/prometheus', '19090:9090'],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    server = {}
    for f in (ROOT / 'results/expB/server-logs').glob('*.jsonl'):
        for r in read_jsonl(f):
            if r.get('event') == 'request':
                server[r['request_id']] = r
    out = []
    for run in runs:
        recs = read_jsonl(ROOT / f'results/expB/requests-{run}.jsonl')
        start = min(r['send_wall'] for r in recs) - 2
        end = max(r['done_wall'] for r in recs) + 6
        win = int(end - start) + 1
        ok = [r for r in recs if r['outcome'] == 'ok']
        srv = [server[r['request_id']] for r in ok if r['request_id'] in server]
        row = dict(run=run, window_s=win, start=start, end=end,
                   n_client_ok=len(ok), n_server_ok=len(srv),
                   n_prom=q(f'sum(increase(sim_ttfc_seconds_count[{win}s]))', end))
        for p in (.5, .95):
            expr = f'histogram_quantile({p}, sum by (le) (increase(sim_ttfc_seconds_bucket[{win}s])))'
            row[f'promql_p{int(p*100)}'] = q(expr, end)
            row[f'server_p{int(p*100)}'] = round(nearest_rank([r['ttfc_s'] for r in srv], p), 4) if srv else None
            row[f'client_p{int(p*100)}'] = round(nearest_rank([r['ttfc_s'] for r in ok], p), 4)
            row[f'server_p{int(p*100)}_bucket'] = bucket_of(row[f'server_p{int(p*100)}']) if srv else None
        row['promql_expr'] = 'histogram_quantile(0.95, sum by (le) (increase(sim_ttfc_seconds_bucket[WINDOWs])))'
        out.append(row)
        print(json.dumps(row, ensure_ascii=False))
    pf.terminate()
    (ROOT / 'results/tables').mkdir(parents=True, exist_ok=True)
    json.dump(out, open(ROOT / 'results/tables/promql-vs-client.json', 'w'), ensure_ascii=False, indent=1)


if __name__ == '__main__':
    main(sys.argv[1:] or ['B-router-uniform-r3', 'B-clientip-skew80-r3', 'B-default-skew80-r3'])
