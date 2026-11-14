# ДЗ 11. Сервис работает, пользователи ждут — Kubernetes под нагрузкой

Продолжение сервиса ДЗ 8+9+10 (`api.py` / `backends.py`): тот же интерфейс backend
(`load → warmup → generate`), проверка хеша артефакта перенесена из `release.verify_files`.
Для опытов используется **лёгкий управляемый CPU-backend `backend_mode=simulated`** (имитатор):
он один и тот же во всех сравниваемых прогонах. Настоящая модель не подключалась, поэтому
все задержки называются **TTFC** (время до первой части ответа) и **inter-chunk latency**;
TTFT/TPOT/tokens/s в работе не используются.

**Отчёт:** [`report.html`](report.html) (то же в [`report.pdf`](report.pdf)) · ревью PR — раздел 7 отчёта и `docs/review-pr-pin-dialog.md` · сырые результаты: `results/requests.jsonl`, `results/requests.csv`
· графики: `results/plots/` · журналы опытов: `results/expA|expB|expC/`.

## Состав

| Путь | Что это |
|---|---|
| `app/server.py` | FastAPI-сервис: `/health` (liveness), `/startup`, `/ready`, `/drain` (preStop), `/v1/chat` (NDJSON-поток), `/metrics` |
| `app/backend.py` | имитатор: прогрев, CPU-стоимость prefill/частей, **имитация локального префиксного кеша (LRU)** |
| `app/artifact.py` | проверка sha256 и версии файла модели-имитатора (`models/model-A.json`, `model-B.json`) |
| `app/router.py` | исправление опыта B: cache-aware маршрутизатор (rendezvous-хеш по session_id + bounded load) |
| `loadgen/` | детерминированное расписание (`schedule.py`) и клиент с монотонными часами (`client.py`) |
| `charts/llm-sim/` | Helm-чарт: Deployment (2 реплики), 2 Service (None / ClientIP), ConfigMap, router, 4 клиентских Pod, HPA |
| `charts/monitoring/` | Prometheus (pod discovery, cAdvisor, kube-state-metrics, 2 алерта), Grafana + дашборд |
| `deploy/` | конфигурация kind, metrics-server v0.8.0 (патч `--kubelet-insecure-tls`) |
| `scripts/` | `up.sh`, `down.sh`, опыты `exp_a.py`, `exp_b.py`, `exp_c.py`, обвязка `kexp.py` |
| `tests/` | собственные проверки: `test_k8s.py` (через Kubernetes), `test_local.py` |
| `analysis/` | графики/таблицы, сверка PromQL с клиентским журналом |
| `Dockerfile`, `requirements.lock` | образ на `python:3.12-slim@sha256:5e2bdb…`, зависимости с хешами |

## Запуск

Требуется: Docker (OrbStack/Docker Desktop), `kind` ≥ 0.33, `kubectl`, `helm` 4, `uv`.

```bash
make up            # kind-кластер hw11 + образы + metrics-server + мониторинг + сервис (≈3 мин, ≈1.5 ГБ скачать)
make test-local    # проверки без кластера
make test-k8s      # проверки через Kubernetes (стенд должен быть поднят)
make exp-a exp-b exp-c plots   # опыты (~1.5 ч) и графики
make grafana       # Grafana http://127.0.0.1:3000 (анонимно), Prometheus http://127.0.0.1:9090
```

Ручной выпуск версий: `helm upgrade sim charts/llm-sim -n llm -f charts/llm-sim/values-B.yaml`,
откат: `helm rollback sim <ревизия> -n llm`. Исходный дефект прогрева:
`-f charts/llm-sim/values-naive-probes.yaml`.

## Удаление стенда

```bash
make down          # = kind delete cluster --name hw11 && docker image rm hw11-sim:0.1.1
# при желании также базовые образы (~1.4 ГБ):
docker image rm prom/prometheus:v3.5.0 grafana/grafana:12.1.1 kindest/node:v1.37.0 \
  registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.16.0 registry.k8s.io/metrics-server/metrics-server:v0.8.0
```

Версии, лимиты и фактический размер стенда — в `results/env/` и в разделе «Стенд» отчёта.

## Основные результаты (backend_mode=simulated)

| Опыт | До | После |
|---|---|---|
| A. Прогрев (Pod-замена под нагрузкой) | naive probes: 16 запросов на непрогретый Pod, p95 TTFC 14,5 с | `/startup` + `/ready`: 0 запросов, p95 2,7 с |
| A. Rollout A→B с длительными потоками | — | 198/198 ok, 0 5xx, 0 обрывов; потоки 50 и 67 с завершились при drain |
| A. Неготовая B → `helm rollback` | B не стала Ready (sha256), трафик только на A | после отката 72/72 ответа версии A |
| B. ClientIP, 80% через одного клиента | p95 TTFC 14,5 с, 28% 429, 90% запросов на один Pod | cache-aware router: p95 3,0 с, 0 отказов, 50/50 |
| C. HPA 2→3→2 | — | Ready нового Pod через 23,6 с, возврат к 2 через 106 с после снижения нагрузки |
| C. Горячая реплика при HPA | среднее CPU 119% → 3 реплики | 3-й Pod получил 0 запросов (ClientIP) |

Проверки: `results/checks/` (на исходном дефекте прогрева `test_1` падает, после исправления проходит).
