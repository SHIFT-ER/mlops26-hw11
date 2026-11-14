"""HTTP-сервис ДЗ 11 (развитие api.py из ДЗ 8+9+10): probes, очередь, лимит, поток частей, метрики.

Эндпоинты:
  GET  /health   liveness  — процесс и цикл событий живы, обработка не зависла;
  GET  /startup  startup   — артефакт проверен и прогрев завершён (один раз за жизнь процесса);
  GET  /ready    readiness — прогрет и не в режиме drain: можно давать рабочий трафик;
  GET  /load     текущие running/waiting (для маршрутизатора);
  GET  /version  загруженная версия, sha256, backend_mode;
  GET  /metrics  Prometheus;
  POST /drain    preStop: снять Pod с трафика и подождать обновления EndpointSlice;
  POST /admin/reset-cache  очистить локальный префиксный кеш (явное начальное состояние опыта);
  POST /v1/chat  ответ частями (NDJSON) или целиком; история диалога приходит в запросе.
"""
import asyncio
import json
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field

from .backend import SimulatedBackend
from .config import Config

# ---------- метрики: ограниченный набор меток (версия, исход), pod добавляет Prometheus ----------
LAT_BUCKETS = (.01, .025, .05, .1, .2, .3, .5, .75, 1, 1.5, 2, 3, 4, 6, 8, 12, 20, 30, 45, 60)
TTFC = Histogram('sim_ttfc_seconds', 'Время от приёма запроса до первой части ответа (TTFC)',
                 ['model_version'], buckets=LAT_BUCKETS)
DURATION = Histogram('sim_request_duration_seconds', 'Длительность полного ответа',
                     ['model_version', 'outcome'], buckets=LAT_BUCKETS)
QUEUE_WAIT = Histogram('sim_queue_wait_seconds', 'Ожидание свободного слота обработки',
                       ['model_version'], buckets=LAT_BUCKETS)
INTER_CHUNK = Histogram('sim_inter_chunk_seconds', 'Интервал между частями ответа (inter-chunk latency)',
                        ['model_version'], buckets=(.002, .005, .01, .02, .03, .05, .075, .1, .2, .5, 1, 2))
REQUESTS = Counter('sim_requests_total', 'Запросы по исходу', ['model_version', 'outcome'])
CACHE = Counter('sim_prefix_cache_lookups_total', 'Обращения к имитации префиксного кеша', ['result'])
RUNNING = Gauge('sim_running', 'Запросы в обработке')
WAITING = Gauge('sim_waiting', 'Запросы в очереди')
READY = Gauge('sim_ready', '1 — Pod готов к трафику (прогрет и не drain)')
WARM = Gauge('sim_warm', '1 — прогрев завершён')
INFO = Gauge('sim_model_info', 'Загруженная версия', ['model_version', 'model_sha256', 'backend_mode'])


class Message(BaseModel):
    model_config = ConfigDict(extra='forbid')
    role: str = Field(pattern='^(system|user|assistant)$')
    content: str = Field(min_length=1, max_length=20000)


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    session_id: str = Field(min_length=1, max_length=128)
    messages: list[Message] = Field(min_length=1, max_length=200)
    max_chunks: int = Field(default=16, ge=1)
    stream: bool = True


def log(event: dict) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + '\n')
    sys.stdout.flush()


class State:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.backend = SimulatedBackend(cfg)
        self.status, self.error = 'starting', None
        self.warm = False
        self.draining = False
        self.running = 0
        self.waiting = 0
        self.last_progress = time.monotonic()
        self.stop = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=cfg.max_concurrency + 1, thread_name_prefix='compute')
        self.slots: asyncio.Semaphore | None = None
        self.warm_event: asyncio.Event | None = None
        self.loop: asyncio.AbstractEventLoop | None = None

    def load(self):
        try:
            self.status = 'loading'
            self.backend.load()
            INFO.labels(self.backend.version, self.backend.sha256, self.backend.kind).set(1)
            self.status = 'warming'
            log(dict(event='warmup_start', pod=self.cfg.pod_name, model_version=self.backend.version,
                     warmup_seconds=self.cfg.warmup_seconds, ts=time.time()))
            self.backend.warmup(self.cfg.warmup_seconds, self.stop)
            self.warm, self.status = True, 'ready'
            WARM.set(1)
            self._update_ready()
            self.loop.call_soon_threadsafe(self.warm_event.set)
            log(dict(event='warmup_done', pod=self.cfg.pod_name, model_version=self.backend.version, ts=time.time()))
        except Exception as exc:  # процесс жив, но никогда не станет готов: причина видна в /startup
            self.status, self.error = 'failed', f'{type(exc).__name__}: {exc}'
            log(dict(event='load_failed', pod=self.cfg.pod_name, error=self.error, ts=time.time()))

    def _update_ready(self):
        READY.set(1 if self.ready else 0)

    @property
    def ready(self):
        return self.warm and not self.draining

    def ident(self):
        return dict(pod=self.cfg.pod_name, model_version=self.backend.version or self.cfg.model_version,
                    model_sha256=self.backend.sha256, backend_mode=self.backend.kind)


def create_app(cfg: Config | None = None) -> FastAPI:
    cfg = cfg or Config()
    state = State(cfg)

    @asynccontextmanager
    async def lifespan(app):
        state.loop = asyncio.get_running_loop()
        state.slots = asyncio.Semaphore(cfg.max_concurrency)
        state.warm_event = asyncio.Event()
        with open('/tmp/app.pid', 'w') as f:
            f.write(str(os.getpid()))
        threading.Thread(target=state.load, name='model-load', daemon=True).start()
        log(dict(event='process_start', pod=cfg.pod_name, pid=os.getpid(), ts=time.time()))
        yield
        state.stop.set()
        log(dict(event='process_stop', pod=cfg.pod_name, running=state.running, ts=time.time()))

    app = FastAPI(title='hw11 simulated LLM service', lifespan=lifespan)
    app.state.service = state

    @app.middleware('http')
    async def close_when_draining(request: Request, call_next):
        response = await call_next(request)
        if state.draining:  # клиенты не переиспользуют keep-alive соединение со снимаемым Pod
            response.headers['Connection'] = 'close'
        return response

    @app.get('/health')
    def health():
        stalled = state.running > 0 and time.monotonic() - state.last_progress > cfg.liveness_stall_seconds
        if stalled:
            return JSONResponse(status_code=500, content=dict(alive=False, reason='compute stalled'))
        return dict(alive=True, status=state.status, **state.ident())

    @app.get('/startup')
    def startup():
        if not state.warm:
            return JSONResponse(status_code=503, content=dict(started=False, status=state.status, error=state.error))
        return dict(started=True, **state.ident())

    @app.get('/ready')
    def ready():
        if not state.ready:
            return JSONResponse(status_code=503, content=dict(ready=False, status=state.status,
                                                              draining=state.draining, error=state.error))
        return dict(ready=True, **state.ident())

    @app.get('/load')
    def load():
        return dict(running=state.running, waiting=state.waiting, ready=state.ready,
                    max_concurrency=cfg.max_concurrency, max_waiting=cfg.max_waiting, **state.ident())

    @app.get('/version')
    def version():
        return dict(status=state.status, warm=state.warm, cache_entries=len(state.backend.cache), **state.ident())

    @app.get('/metrics')
    def metrics():
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.api_route('/drain', methods=['GET', 'POST'])  # preStop httpGet делает GET
    async def drain():
        state.draining = True
        state._update_ready()
        log(dict(event='drain_start', pod=cfg.pod_name, running=state.running, waiting=state.waiting, ts=time.time()))
        await asyncio.sleep(cfg.drain_seconds)  # preStop: ждём, пока Pod исчезнет из EndpointSlice
        log(dict(event='drain_done', pod=cfg.pod_name, running=state.running, ts=time.time()))
        return dict(draining=True)

    @app.post('/admin/reset-cache')
    def reset_cache():
        state.backend.cache.clear()
        return dict(cleared=True, pod=cfg.pod_name)

    @app.post('/v1/chat')
    async def chat(body: ChatRequest, request: Request):
        received = time.perf_counter()
        rid = request.headers.get('x-request-id') or str(uuid.uuid4())
        peer = request.client.host if request.client else None
        base = dict(event='request', request_id=rid, session_id=body.session_id, peer_ip=peer,
                    ts_received=time.time(), **state.ident())
        headers = {'X-Request-ID': rid, 'X-Pod': cfg.pod_name,
                   'X-Model-Version': state.backend.version or cfg.model_version, 'X-Backend-Mode': 'simulated'}
        version = state.backend.version or cfg.model_version

        def reject(code, outcome, error):
            REQUESTS.labels(version, outcome).inc()
            log(dict(base, status=code, outcome=outcome, error=error))
            return JSONResponse(status_code=code, headers=headers,
                                content=dict(error=error, outcome=outcome, request_id=rid, **state.ident()))

        if body.max_chunks > cfg.max_chunks:
            return reject(422, 'invalid', f'max_chunks > {cfg.max_chunks}')
        if state.status == 'failed':
            return reject(503, 'not_ready', state.error)
        if state.waiting >= cfg.max_waiting:  # допуск: очередь ограничена, лишнее — сразу 429
            return reject(429, 'rejected', 'queue is full')

        warm_at_receive = state.warm
        messages = [m.model_dump() for m in body.messages]
        state.waiting += 1
        WAITING.set(state.waiting)
        acquired = False
        try:
            if not state.warm:  # сюда запрос попадает только при неверной readiness (исходный дефект)
                try:
                    await asyncio.wait_for(state.warm_event.wait(), cfg.warm_wait_timeout)
                except asyncio.TimeoutError:
                    return reject(503, 'not_ready', 'warmup timeout')
            await state.slots.acquire()
            acquired = True
        finally:
            state.waiting -= 1
            WAITING.set(state.waiting)
        queue_s = time.perf_counter() - received
        QUEUE_WAIT.labels(version).observe(queue_s)
        state.running += 1
        RUNNING.set(state.running)

        loop = asyncio.get_running_loop()
        out: asyncio.Queue = asyncio.Queue()
        cancel = threading.Event()

        def work():
            try:
                info = state.backend.prefill(messages)
                loop.call_soon_threadsafe(out.put_nowait, ('meta', info))
                for text in state.backend.chunks(messages, body.max_chunks):
                    if cancel.is_set():
                        break
                    state.last_progress = time.monotonic()
                    loop.call_soon_threadsafe(out.put_nowait, ('chunk', text))
                loop.call_soon_threadsafe(out.put_nowait, ('end', None))
            except Exception as exc:  # noqa: BLE001
                loop.call_soon_threadsafe(out.put_nowait, ('error', f'{type(exc).__name__}: {exc}'))

        state.last_progress = time.monotonic()
        state.executor.submit(work)
        rec = dict(base, warm_at_receive=warm_at_receive, queue_s=round(queue_s, 4), max_chunks=body.max_chunks)

        async def produce():
            n, ttfc, last, outcome, parts = 0, None, None, 'aborted', []
            try:
                while True:
                    kind, value = await out.get()
                    now = time.perf_counter()
                    if kind == 'meta':
                        CACHE.labels(value['cache']).inc()
                        rec.update(value)
                        yield dict(type='meta', request_id=rid, queue_s=round(queue_s, 4),
                                   warm_at_receive=warm_at_receive, **value, **state.ident())
                    elif kind == 'chunk':
                        if ttfc is None:
                            ttfc = now - received
                            TTFC.labels(version).observe(ttfc)
                        else:
                            INTER_CHUNK.labels(version).observe(now - last)
                        last = now
                        n += 1
                        parts.append(value)
                        yield dict(type='chunk', i=n - 1, text=value)
                    elif kind == 'end':
                        outcome = 'ok'
                        yield dict(type='done', chunks=n, request_id=rid, **state.ident())
                        break
                    else:
                        outcome = 'error'
                        yield dict(type='error', error=value, request_id=rid)
                        break
            finally:  # сюда попадаем и при обрыве клиентом (GeneratorExit / CancelledError)
                cancel.set()
                if acquired:
                    state.slots.release()
                state.running -= 1
                RUNNING.set(state.running)
                total = time.perf_counter() - received
                REQUESTS.labels(version, outcome).inc()
                DURATION.labels(version, outcome).observe(total)
                rec.update(status=200, outcome=outcome, chunks=n, ttfc_s=None if ttfc is None else round(ttfc, 4),
                           duration_s=round(total, 4), ts_done=time.time(), draining=state.draining)
                log(rec)
                rec['text'] = ''.join(parts)

        if body.stream:
            async def ndjson():
                async for item in produce():
                    yield json.dumps(item, ensure_ascii=False) + '\n'
            return StreamingResponse(ndjson(), media_type='application/x-ndjson', headers=headers)

        items = [item async for item in produce()]
        done = items[-1]
        status = 200 if done['type'] == 'done' else 500
        return JSONResponse(status_code=status, headers=headers, content=dict(
            request_id=rid, session_id=body.session_id, text=rec.get('text', ''), chunks=rec.get('chunks'),
            ttfc_s=rec.get('ttfc_s'), queue_s=rec['queue_s'], cache=rec.get('cache'),
            warm_at_receive=warm_at_receive, **state.ident()))

    return app


def main():
    import uvicorn
    cfg = Config()
    app = create_app(cfg)
    # timeout_graceful_shutdown < terminationGracePeriodSeconds: активные потоки успевают завершиться
    uvicorn.run(app, host='0.0.0.0', port=cfg.port, log_level='warning', access_log=False,
                timeout_keep_alive=30, timeout_graceful_shutdown=cfg.graceful_seconds)


if __name__ == '__main__':
    main()
