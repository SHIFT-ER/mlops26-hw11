# ДЗ 11 — Kubernetes под нагрузкой. Все цели запускаются из корня репозитория.
.PHONY: up down test-local test-k8s exp-a exp-b exp-c plots size

up:            ## kind + образы + metrics-server + мониторинг + сервис (≈3 мин при скачанных образах)
	./scripts/up.sh
down:          ## удалить кластер kind и образ сервиса
	./scripts/down.sh
test-local:    ## проверки без кластера (учёт отказов, маршрутизатор, кеш, артефакт)
	uv run pytest tests/test_local.py -v
test-k8s:      ## проверки, обращающиеся к сервису в Kubernetes
	uv run pytest tests/test_k8s.py -v
exp-a:         ## опыт A: прогрев (дефект/исправление), rollout A->B, неготовая B + откат, kill -9
	python3 scripts/exp_a.py warm-naive && python3 scripts/exp_a.py warm-fixed && \
	python3 scripts/exp_a.py rollout && python3 scripts/exp_a.py broken && python3 scripts/exp_a.py kill
exp-b:         ## опыт B: 3 политики x 2 трафика x 3 прогона (~55 мин)
	python3 scripts/exp_b.py
exp-c:         ## опыт C: HPA 2->3->2 и горячая реплика при HPA
	python3 scripts/exp_c.py scale && python3 scripts/exp_c.py hot
plots:         ## графики, таблицы, results/requests.{jsonl,csv}
	uv run python analysis/plots.py
size:          ## фактический размер стенда
	docker system df; docker exec hw11-control-plane df -h /; docker exec hw11-control-plane crictl images
grafana:       ## http://127.0.0.1:3000 (анонимный доступ), Prometheus http://127.0.0.1:9090
	kubectl -n monitoring port-forward svc/grafana 3000:3000 & kubectl -n monitoring port-forward svc/prometheus 9090:9090
