"""Генерирует дашборд Grafana (charts/monitoring/files/llm-sim-dashboard.json) из списка панелей."""
import json

P = [  # (заголовок, единица, [(expr, legend)])
    ('TTFC p50/p95 по Pod (classic histogram, окно 1m)', 's', [
        ('histogram_quantile(0.95, sum by (le, pod) (rate(sim_ttfc_seconds_bucket[1m])))', 'p95 {{pod}}'),
        ('histogram_quantile(0.5, sum by (le, pod) (rate(sim_ttfc_seconds_bucket[1m])))', 'p50 {{pod}}')]),
    ('Длительность полного ответа p50/p95 (outcome=ok)', 's', [
        ('histogram_quantile(0.95, sum by (le) (rate(sim_request_duration_seconds_bucket{outcome="ok"}[1m])))', 'p95'),
        ('histogram_quantile(0.5, sum by (le) (rate(sim_request_duration_seconds_bucket{outcome="ok"}[1m])))', 'p50')]),
    ('Running / waiting по Pod', 'short', [
        ('sum by (pod) (sim_running{app="llm-sim"})', 'running {{pod}}'),
        ('sum by (pod) (sim_waiting{app="llm-sim"})', 'waiting {{pod}}')]),
    ('Завершённые запросы, 1/с', 'reqps', [
        ('sum by (pod) (rate(sim_requests_total{outcome="ok"}[1m]))', '{{pod}}')]),
    ('Ошибки и отказы, 1/с', 'reqps', [
        ('sum by (outcome) (rate(sim_requests_total{outcome!="ok"}[1m]))', '{{outcome}}')]),
    ('Доля отказов', 'percentunit', [
        ('sum(rate(sim_requests_total{outcome!="ok"}[1m])) / sum(rate(sim_requests_total[1m]))', 'error ratio')]),
    ('CPU по Pod, ядра (cAdvisor)', 'short', [
        ('sum by (pod) (rate(container_cpu_usage_seconds_total{container="sim"}[1m]))', '{{pod}}')]),
    ('RAM по Pod (working set)', 'bytes', [
        ('sum by (pod) (container_memory_working_set_bytes{container="sim"})', '{{pod}}')]),
    ('Ready-реплики и HPA', 'short', [
        ('kube_deployment_status_replicas_ready{deployment="llm-sim"}', 'ready'),
        ('kube_deployment_spec_replicas{deployment="llm-sim"}', 'spec'),
        ('kube_horizontalpodautoscaler_status_desired_replicas', 'hpa desired')]),
    ('Префиксный кеш (имитация): доля hit', 'percentunit', [
        ('sum(rate(sim_prefix_cache_lookups_total{result="hit"}[1m])) / sum(rate(sim_prefix_cache_lookups_total[1m]))', 'hit ratio')]),
    ('Inter-chunk latency p95', 's', [
        ('histogram_quantile(0.95, sum by (le) (rate(sim_inter_chunk_seconds_bucket[1m])))', 'p95')]),
    ('Алерты (ALERTS)', 'short', [('ALERTS', '{{alertname}} {{alertstate}}')]),
]
panels = []
for i, (title, unit, targets) in enumerate(P):
    panels.append(dict(
        id=i + 1, type='timeseries', title=title, datasource=dict(type='prometheus', uid='prom'),
        gridPos=dict(h=8, w=12, x=12 * (i % 2), y=8 * (i // 2)),
        fieldConfig=dict(defaults=dict(unit=unit, custom=dict(lineWidth=1, showPoints='never')), overrides=[]),
        options=dict(legend=dict(displayMode='list', placement='bottom'), tooltip=dict(mode='multi')),
        targets=[dict(refId=chr(65 + j), expr=e, legendFormat=l, datasource=dict(type='prometheus', uid='prom'))
                 for j, (e, l) in enumerate(targets)]))
dash = dict(uid='hw11-llm-sim', title='ДЗ 11 · llm-sim (backend_mode=simulated)', schemaVersion=41, version=1,
            refresh='5s', time=dict(**{'from': 'now-30m', 'to': 'now'}), tags=['hw11'], panels=panels)
json.dump(dash, open('charts/monitoring/files/llm-sim-dashboard.json', 'w'), ensure_ascii=False, indent=1)
print(len(panels), 'panels')
