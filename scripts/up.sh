#!/usr/bin/env bash
# Повторяемый запуск стенда ДЗ 11: kind -> образы -> metrics-server -> мониторинг -> сервис.
set -euo pipefail
cd "$(dirname "$0")/.."
TAG=${TAG:-0.1.1}
IMAGES="prom/prometheus:v3.5.0 grafana/grafana:12.1.1 registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.16.0 registry.k8s.io/metrics-server/metrics-server:v0.8.0"
kind get clusters | grep -qx hw11 || kind create cluster --config deploy/kind.yaml --wait 120s
docker build -t hw11-sim:$TAG .
for i in $IMAGES; do docker image inspect "$i" >/dev/null 2>&1 || docker pull "$i"; done
kind load docker-image --name hw11 hw11-sim:$TAG $IMAGES
kubectl apply -f deploy/metrics-server-v0.8.0.yaml
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace llm --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install monitoring charts/monitoring -n monitoring --wait --timeout 5m
helm upgrade --install sim charts/llm-sim -n llm --set image.tag=$TAG --wait --timeout 5m
kubectl -n llm get pods -o wide
