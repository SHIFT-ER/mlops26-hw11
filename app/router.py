"""Маршрутизатор «cache-aware + bounded load» перед репликами имитатора (исправление опыта B).

Ключ привязки — session_id (диалог), а не IP клиента. Pod выбирается rendezvous-хешем по
списку Ready-адресов из EndpointSlice (Kubernetes API), поэтому один диалог стабильно идёт
в реплику, где уже лежит его префикс. Если у предпочтительной реплики в работе больше
запросов, чем у наименее загруженной, на BALANCE_THRESHOLD и более, запрос уходит в
наименее загруженную: ожидание в очереди дороже пересчёта префикса (идея cache-aware
router SGLang: локальность кеша, пока дисбаланс нагрузки ниже порога).

Политики ROUTER_POLICY: cache_aware (по умолчанию), least_loaded, session_hash.
"""
import asyncio
import hashlib
import json
import os
import ssl
import sys
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

SA = '/var/run/secrets/kubernetes.io/serviceaccount'
NAMESPACE = os.environ.get('NAMESPACE') or (open(f'{SA}/namespace').read().strip() if os.path.exists(SA) else 'default')
SERVICE = os.environ.get('BACKEND_SERVICE', 'llm-sim')
PORT = int(os.environ.get('BACKEND_PORT', '8000'))
POLICY = os.environ.get('ROUTER_POLICY', 'cache_aware')
THRESHOLD = int(os.environ.get('BALANCE_THRESHOLD', '2'))
REFRESH = float(os.environ.get('REFRESH_SECONDS', '1'))

DECISIONS = Counter('router_decisions_total', 'Решения маршрутизатора', ['reason'])
ENDPOINTS = Gauge('router_ready_endpoints', 'Ready-адреса в EndpointSlice')


def log(event):
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + '\n')
    sys.stdout.flush()


def rendezvous(key: str, endpoints: list[str]) -> list[str]:
    return sorted(endpoints, key=lambda e: hashlib.sha256(f'{key}|{e}'.encode()).hexdigest(), reverse=True)


def choose(policy: str, session: str, endpoints: list[str], inflight: dict, threshold: int) -> tuple[str, str]:
    """Чистая функция выбора (покрыта юнит-тестом). Возвращает (адрес, причина)."""
    ranked = rendezvous(session, endpoints)
    if policy == 'session_hash':
        return ranked[0], 'affinity'
    least = min(ranked, key=lambda e: (inflight.get(e, 0), ranked.index(e)))
    if policy == 'least_loaded':
        return least, 'least_loaded'
    preferred = ranked[0]
    if inflight.get(preferred, 0) - inflight.get(least, 0) >= threshold:
        return least, 'overflow'
    return preferred, 'affinity'


class Discovery:
    """Список Ready-адресов сервиса из EndpointSlice через API-сервер (а не DNS с кешем 30 с)."""

    def __init__(self):
        self.endpoints: dict[str, str] = {}  # ip -> pod
        self.token = open(f'{SA}/token').read().strip() if os.path.exists(SA) else None
        ctx = ssl.create_default_context(cafile=f'{SA}/ca.crt') if self.token else None
        self.client = httpx.AsyncClient(verify=ctx or False, timeout=3)

    async def refresh(self):
        url = (f'https://kubernetes.default.svc/apis/discovery.k8s.io/v1/namespaces/{NAMESPACE}/endpointslices'
               f'?labelSelector=kubernetes.io/service-name={SERVICE}')
        r = await self.client.get(url, headers={'Authorization': f'Bearer {self.token}'})
        r.raise_for_status()
        found = {}
        for sl in r.json().get('items', []):
            for ep in sl.get('endpoints') or []:
                if (ep.get('conditions') or {}).get('ready'):
                    for ip in ep.get('addresses', []):
                        found[ip] = (ep.get('targetRef') or {}).get('name', ip)
        if found != self.endpoints:
            log(dict(event='endpoints', ts=time.time(), ready=sorted(found.values())))
        self.endpoints = found
        ENDPOINTS.set(len(found))


def create_app() -> FastAPI:
    disc = Discovery()
    inflight: dict[str, int] = {}
    upstream = httpx.AsyncClient(timeout=httpx.Timeout(120, connect=2),
                                 limits=httpx.Limits(max_connections=200, max_keepalive_connections=50))

    async def loop():
        while True:
            try:
                await disc.refresh()
            except Exception as exc:  # noqa: BLE001
                log(dict(event='discovery_error', error=str(exc), ts=time.time()))
            await asyncio.sleep(REFRESH)

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(loop())
        yield
        task.cancel()

    app = FastAPI(title='hw11 cache-aware router', lifespan=lifespan)

    @app.get('/health')
    def health():
        return dict(alive=True)

    @app.get('/ready')
    def ready():
        if not disc.endpoints:
            return JSONResponse(status_code=503, content=dict(ready=False))
        return dict(ready=True, endpoints=disc.endpoints, inflight=inflight, policy=POLICY)

    @app.get('/metrics')
    def metrics():
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.post('/v1/chat')
    async def chat(request: Request):
        raw = await request.body()
        body = json.loads(raw)
        rid = request.headers.get('x-request-id') or str(uuid.uuid4())
        tried = set()
        while True:
            eps = [e for e in disc.endpoints if e not in tried]
            if not eps:
                return JSONResponse(status_code=503, content=dict(error='no ready endpoints', request_id=rid))
            ip, reason = choose(POLICY, str(body.get('session_id', rid)), eps, inflight, THRESHOLD)
            tried.add(ip)
            inflight[ip] = inflight.get(ip, 0) + 1
            try:
                req = upstream.build_request('POST', f'http://{ip}:{PORT}/v1/chat', content=raw,
                                             headers={'content-type': 'application/json', 'x-request-id': rid})
                resp = await upstream.send(req, stream=True)
            except httpx.TransportError:  # соединение не установлено — ответ не начат, можно в другую
                inflight[ip] -= 1
                continue
            DECISIONS.labels(reason).inc()
            headers = {k: v for k, v in resp.headers.items() if k.lower().startswith('x-')}
            headers['X-Route-Reason'] = reason

            async def relay(resp=resp, ip=ip):
                try:
                    async for part in resp.aiter_raw():
                        yield part
                finally:
                    await resp.aclose()
                    inflight[ip] -= 1

            return StreamingResponse(relay(), status_code=resp.status_code, headers=headers,
                                     media_type=resp.headers.get('content-type'))

    return app


def main():
    import uvicorn
    uvicorn.run(create_app(), host='0.0.0.0', port=int(os.environ.get('PORT', '8000')),
                log_level='warning', access_log=False, timeout_keep_alive=30)


if __name__ == '__main__':
    main()
