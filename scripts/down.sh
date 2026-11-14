#!/usr/bin/env bash
# Удаление собственного стенда: кластер kind целиком и локальный образ сервиса.
set -euo pipefail
kind delete cluster --name hw11
docker image rm hw11-sim:${TAG:-0.1.1} || true
# Образы мониторинга и kindest/node общие; удалить при желании:
#   docker image rm prom/prometheus:v3.5.0 grafana/grafana:12.1.1 \
#     registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.16.0 registry.k8s.io/metrics-server/metrics-server:v0.8.0
#   docker image prune
