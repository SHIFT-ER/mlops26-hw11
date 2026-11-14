"""Графики и сводные таблицы из сырых результатов results/ (только чтение сырья, без кластера).

  uv run python analysis/plots.py
Выход: results/plots/*.png, results/tables/*.json, results/requests.jsonl + requests.csv (все попытки).
"""
import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
R = ROOT / 'results'
OUT = R / 'plots'
TAB = R / 'tables'
sys.path.insert(0, str(ROOT / 'scripts'))
from kexp import read_jsonl, summarize  # noqa: E402

plt.rcParams.update({'font.size': 9, 'axes.grid': True, 'grid.alpha': .3, 'figure.dpi': 130})
POL = {'default': 'Service (None)', 'clientip': 'Service ClientIP', 'router': 'cache-aware router'}
COL = {'default': '#4C72B0', 'clientip': '#DD8452', 'router': '#55A868'}


def pct(vals, q):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    k = max(0, min(len(vals) - 1, int(round(q * len(vals) + 0.5)) - 1))
    return vals[k]


def short(pod):
    return (pod or '?')[-5:]


# ------------------------------------------------------------------ опыт B
def exp_b():
    d = R / 'expB'
    rows = read_jsonl(d / 'summary.jsonl') if (d / 'summary.jsonl').exists() else []
    if not rows:
        return
    table, perpod = [], []
    for row in rows:
        recs = [r for r in read_jsonl(d / f'requests-{row["run"]}.jsonl') if r['phase'] == 'measure']
        pods = sorted({r['pod'] for r in recs if r.get('pod')})
        ent = dict(run=row['run'], policy=row['policy'], traffic=row['traffic'], rep=row['rep'], **summarize(recs))
        ent['cache_hit'] = round(sum(r.get('cache') == 'hit' for r in recs) / max(1, sum(bool(r.get('cache')) for r in recs)), 3)
        ent['per_pod'] = {}
        for p in pods:
            pr = [r for r in recs if r.get('pod') == p]
            ok = [r for r in pr if r['outcome'] == 'ok']
            ent['per_pod'][short(p)] = dict(
                requests=len(pr), share=round(len(pr) / len(recs), 3), ok=len(ok),
                rejected=sum(r['outcome'] == 'rejected' for r in pr),
                ttfc_p50=pct([r['ttfc_s'] for r in ok], .5), ttfc_p95=pct([r['ttfc_s'] for r in ok], .95),
                queue_p95=pct([r['queue_s'] for r in ok], .95),
                clients=dict(Counter(r['client'] for r in pr)))
        ent['client_to_pod'] = {c: dict(Counter(short(r.get('pod')) for r in recs if r['client'] == c))
                                for c in range(4)}
        table.append(ent)
    TAB.mkdir(parents=True, exist_ok=True)
    json.dump(table, open(TAB / 'expB-runs.json', 'w'), ensure_ascii=False, indent=1)

    # агрегат: среднее и разброс по трём повторам
    agg = defaultdict(list)
    for e in table:
        agg[(e['policy'], e['traffic'])].append(e)
    summary = []
    for (pol, tr), es in sorted(agg.items()):
        summary.append(dict(policy=pol, traffic=tr, runs=len(es),
                            ttfc_p50=[e['ttfc_p50'] for e in es], ttfc_p95=[e['ttfc_p95'] for e in es],
                            dur_p95=[e['dur_p95'] for e in es], error_share=[e['error_share'] for e in es],
                            n_ok=[e['ok'] for e in es], attempts=[e['attempts'] for e in es],
                            cache_hit=[e['cache_hit'] for e in es],
                            max_pod_share=[max(p['share'] for p in e['per_pod'].values()) for e in es]))
    json.dump(summary, open(TAB / 'expB-summary.json', 'w'), ensure_ascii=False, indent=1)

    # график 3: сравнение политик по трём прогонам
    fig, axes = plt.subplots(1, 4, figsize=(14, 3.6))
    metrics = [('ttfc_p50', 'TTFC p50, с'), ('ttfc_p95', 'TTFC p95, с'), ('error_share', 'доля отказов (429)'),
               ('max_pod_share', 'доля запросов на самый загруженный Pod')]
    for ax, (m, title) in zip(axes, metrics):
        for i, tr in enumerate(['uniform', 'skew80']):
            for j, pol in enumerate(POL):
                s = next((x for x in summary if x['policy'] == pol and x['traffic'] == tr), None)
                if not s:
                    continue
                xs = i * 4 + j
                vals = [v for v in s[m] if v is not None]
                ax.bar(xs, statistics.mean(vals) if vals else 0, color=COL[pol], alpha=.45,
                       label=POL[pol] if i == 0 else None)
                ax.scatter([xs] * len(vals), vals, color=COL[pol], s=14, zorder=3, edgecolor='k', linewidth=.4)
        ax.set_xticks([1, 5], ['uniform', '80% через клиента 0'])
        ax.set_title(title)
        if m.startswith('ttfc'):
            ax.set_yscale('log')
    axes[0].legend(fontsize=7, loc='upper left')
    fig.suptitle('Опыт B: 3 политики x 2 вида трафика x 3 прогона (точки — прогоны, столбец — среднее); '
                 '2 реплики, 2.5 запр/с, warmup 30 с + измерение 120 с, кеш очищен перед прогоном, клиенты с keep-alive; backend_mode=simulated',
                 fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT / '3-policies-3runs.png')
    plt.close(fig)

    # график 2: задержка и очередь по Pod во времени (skew80, повтор 1, ClientIP vs router vs default)
    fig, axes = plt.subplots(2, 3, figsize=(14, 6), sharex=True)
    for col, pol in enumerate(POL):
        run = f'B-{pol}-skew80-r1'
        f = d / f'requests-{run}.jsonl'
        if not f.exists():
            continue
        recs = read_jsonl(f)
        t0 = min(r['scheduled_wall'] for r in recs)
        pods = sorted({r['pod'] for r in recs if r.get('pod')})
        for k, p in enumerate(pods):
            pr = [r for r in recs if r.get('pod') == p and r['outcome'] == 'ok']
            axes[0, col].scatter([r['send_wall'] - t0 for r in pr], [r['ttfc_s'] for r in pr], s=5,
                                 label=f'{short(p)} (n={len(pr)})', color=f'C{k}')
            axes[1, col].scatter([r['send_wall'] - t0 for r in pr], [r['queue_s'] for r in pr], s=5, color=f'C{k}')
        rej = [r for r in recs if r['outcome'] == 'rejected']
        axes[0, col].scatter([r['send_wall'] - t0 for r in rej], [25] * len(rej), marker='x', s=10, color='r',
                             label=f'429 (n={len(rej)})')
        axes[0, col].set_title(f'{POL[pol]} · {run}')
        axes[0, col].set_yscale('log')
        axes[0, col].legend(fontsize=7, loc='upper left')
        axes[0, col].axvline(30, color='gray', ls=':', lw=1)
        axes[1, col].axvline(30, color='gray', ls=':', lw=1)
        axes[1, col].set_xlabel('время от начала расписания, с (до 30 с — warmup)')
    axes[0, 0].set_ylabel('TTFC, с (клиент, лог-шкала)')
    axes[1, 0].set_ylabel('ожидание слота на Pod, с')
    fig.suptitle('Опыт B: задержка до первой части и очередь по репликам, трафик 80% через одного клиента', fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT / '2-latency-queue-per-pod.png')
    plt.close(fig)


# ------------------------------------------------------------------ опыт A
def _timeline(path):
    tl = read_jsonl(path)
    return tl


def exp_a():
    parts = [p for p in ['warm-naive', 'warm-fixed', 'rollout'] if (R / 'expA' / p / 'timeline.jsonl').exists()]
    if not parts:
        return
    fig, axes = plt.subplots(len(parts), 1, figsize=(12, 3.4 * len(parts)), squeeze=False)
    for ax, part in zip(axes[:, 0], parts):
        d = R / 'expA' / part
        tl = _timeline(d / 'timeline.jsonl')
        recs = []
        for f in d.glob('requests-*.jsonl'):
            recs += read_jsonl(f)
        ev = read_jsonl(d / 'events.jsonl')
        t0 = min(r['scheduled_wall'] for r in recs)
        ax2 = ax.twinx()
        ts = [s['ts'] - t0 for s in tl]
        if part == 'rollout':
            for v, c, ls, off in [('A', 'tab:blue', '--', 0.04), ('B', 'tab:orange', '-', -0.04)]:
                ax2.step(ts, [sum(p['version'] == v and p['name'] in s['endpoints_ready'] for p in s['pods']) + off
                              for s in tl], where='post', color=c, lw=2, ls=ls, label=f'Ready в EndpointSlice, версия {v}')
        else:
            ax2.step(ts, [len(s['endpoints_ready']) for s in tl], where='post', color='k', lw=1.5,
                     label='Ready-адреса в EndpointSlice')
            ax2.step(ts, [len([p for p in s['pods'] if not p['deleting']]) for s in tl], where='post',
                     color='gray', lw=1, ls='--', label='Pod (без удаляемых)')
        ax2.set_ylim(0, 4.5)
        ax2.set_ylabel('реплики')
        pods = sorted({r.get('pod') for r in recs if r.get('pod')})
        for k, p in enumerate(pods):
            pr = [r for r in recs if r.get('pod') == p and r.get('ttfc_s') is not None]
            cold = [r for r in pr if r.get('warm_at_receive') is False]
            ax.scatter([r['send_wall'] - t0 for r in pr], [r['ttfc_s'] for r in pr], s=6, color=f'C{k}',
                       label=f'{short(p)} {next((r.get("model_version") for r in pr), "")} n={len(pr)}')
            if cold:
                ax.scatter([r['send_wall'] - t0 for r in cold], [r['ttfc_s'] for r in cold], s=30, facecolor='none',
                           edgecolor='r', label=f'принят непрогретым backend: {len(cold)}')
        bad = [r for r in recs if r['outcome'] != 'ok']
        if bad:
            ax.scatter([r['send_wall'] - t0 for r in bad], [0.02] * len(bad), marker='x', color='r', s=25,
                       label=f'не ok: {dict(Counter(r["outcome"] for r in bad))}')
        longs = [r for r in recs if r.get('long')]
        for j, r in enumerate(longs):
            ax.hlines(0.02 * 1.6 ** j, r['send_wall'] - t0, r['done_wall'] - t0, color='m', lw=3)
            ax.text(r['done_wall'] - t0, 0.02 * 1.6 ** j, f" поток {r['chunks']} ч., {r['outcome']}, {short(r.get('pod'))}",
                    fontsize=6.5, va='center', color='m')
        for e in ev:
            if e['event'] in ('delete_pod', 'helm_upgrade_start', 'kill9', 'helm_rollback_start', 'rollout_status_done'):
                ax.axvline(e['ts'] - t0, color='r', ls='--', lw=1)
                ax.text(e['ts'] - t0, ax.get_ylim()[1] if ax.get_yscale() == 'linear' else 30, e['event'], fontsize=7,
                        color='r', va='top')
        ax.set_yscale('log')
        ax.set_ylim(0.01, 40)
        ax.set_ylabel('TTFC, с (лог)')
        ax.set_xlabel('время от начала нагрузки, с')
        s = summarize(recs)
        ax.set_title(f'{part}: попыток {s["attempts"]}, исходы {s["outcomes"]}' +
                     (' · фиолетовые полосы — длительные потоки' if longs else ''), fontsize=9)
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, fontsize=6.5, loc='upper right', ncol=2)
    fig.suptitle('Опыт A: прогрев (исходный дефект и исправление) и rolling update A→B; '
                 'нагрузка 1.2 запр/с через Service llm-sim, соединение на запрос, прогрев 20 с', fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT / '1-warmup-rollout-ready.png')
    plt.close(fig)


# ------------------------------------------------------------------ опыт C
def exp_c():
    d = R / 'expC'
    if not (d / 'timeline.jsonl').exists():
        return
    tl = _timeline(d / 'timeline.jsonl')
    recs = []
    for f in sorted(d.glob('requests-*.jsonl')):
        recs += read_jsonl(f)
    t0 = min(r['scheduled_wall'] for r in recs)
    ev = read_jsonl(d / 'events.jsonl')
    fig, (a1, a2, a3) = plt.subplots(3, 1, figsize=(12, 8), sharex=True)
    ts = [s['ts'] - t0 for s in tl]
    a1.step(ts, [(s.get('hpa') or {}).get('desired') for s in tl], where='post', label='HPA desired')
    a1.step(ts, [len([p for p in s['pods'] if not p['deleting']]) for s in tl], where='post', ls='--', label='Pod')
    a1.step(ts, [len(s['endpoints_ready']) for s in tl], where='post', label='Ready в EndpointSlice')
    a1.set_ylabel('реплики')
    a1.set_ylim(0, 4)
    a1b = a1.twinx()
    a1b.plot(ts, [(s.get('hpa') or {}).get('cpu') for s in tl], color='r', lw=1, label='CPU avg, % от requests (HPA)')
    a1b.axhline(60, color='r', ls=':', lw=1)
    a1b.set_ylabel('CPU % (цель 60)')
    h1, l1 = a1.get_legend_handles_labels()
    h2, l2 = a1b.get_legend_handles_labels()
    a1.legend(h1 + h2, l1 + l2, fontsize=7, loc='upper left')
    # предложенная нагрузка (по расписанию) и завершённые в секунду
    bins = defaultdict(int)
    okb = defaultdict(int)
    for r in recs:
        bins[int(r['scheduled_wall'] - t0) // 10 * 10] += 1
        if r['outcome'] == 'ok':
            okb[int(r['done_wall'] - t0) // 10 * 10] += 1
    xs = sorted(bins)
    a2.step(xs, [bins[x] / 10 for x in xs], where='post', label='запланировано, запр/с')
    a2.step(sorted(okb), [okb[x] / 10 for x in sorted(okb)], where='post', label='завершено ok, запр/с')
    a2.set_ylabel('запр/с (окна 10 с)')
    a2.legend(fontsize=7)
    pods = sorted({r.get('pod') for r in recs if r.get('pod')})
    for k, p in enumerate(pods):
        pr = [r for r in recs if r.get('pod') == p and r['outcome'] == 'ok']
        a3.scatter([r['send_wall'] - t0 for r in pr], [r['ttfc_s'] for r in pr], s=4, color=f'C{k}',
                   label=f'{short(p)} n={len(pr)}')
    a3.set_yscale('log')
    a3.set_ylabel('TTFC, с')
    a3.legend(fontsize=7, ncol=3)
    a3.set_xlabel('время от начала нагрузки, с')
    for e in ev:
        if e['event'].startswith('hpa_') or e['event'] in ('pod_created', 'pod_ready', 'load_drop'):
            for a in (a1, a2, a3):
                a.axvline(e['ts'] - t0, color='gray', ls=':', lw=.8)
            a1.text(e['ts'] - t0, 3.8, e['event'], fontsize=6, rotation=90, va='top')
    fig.suptitle('Опыт C: HPA autoscaling/v2 (min 2, max 3, CPU 60% от 500m), масштабирование 2→3→2 под нагрузкой; '
                 'окно стабилизации scaleDown 60 с', fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT / '4-hpa-2-3-2.png')
    plt.close(fig)


# ------------------------------------------------------------------ все сырые попытки
def merge_raw():
    files = sorted(R.glob('exp*/**/requests-*.jsonl'))
    rows = []
    for f in files:
        exp = f.relative_to(R).parts[0] + ('/' + f.parent.name if f.parent.name != f.relative_to(R).parts[0] else '')
        for r in read_jsonl(f):
            r['experiment'] = exp
            r.pop('text', None)
            rows.append(r)
    with open(R / 'requests.jsonl', 'w') as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + '\n')
    cols = ['experiment', 'run', 'id', 'attempt', 'request_id', 'session', 'turn', 'phase', 'client', 'client_pod',
            'long', 'max_chunks', 'prompt_chars', 'scheduled_wall', 'send_wall', 'send_lag_s', 'status', 'outcome',
            'error', 'pod', 'model_version', 'model_sha256', 'backend_mode', 'route_reason', 'cache', 'cached_chars',
            'queue_s', 'warm_at_receive', 'headers_s', 'ttfc_s', 'inter_chunk_mean_s', 'duration_s', 'done_wall',
            'chunks', 'got_done']
    with open(R / 'requests.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)
    print('raw attempts:', len(rows), 'from', len(files), 'files')


if __name__ == '__main__':
    OUT.mkdir(parents=True, exist_ok=True)
    TAB.mkdir(parents=True, exist_ok=True)
    for fn in (exp_a, exp_b, exp_c, merge_raw):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            print(fn.__name__, 'failed:', repr(exc))
            raise
    print('ok', datetime.now().strftime('%H:%M:%S'))
