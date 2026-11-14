"""Проверки без кластера: учёт отказов/оборванных потоков, маршрутизатор, кеш, артефакт.

check 4: генератор нагрузки против поддельного сервера, который отвечает 200, 429, 500,
обрывает поток посередине и молчит дольше тайм-аута, — сводка обязана разнести попытки по
исходам и посчитать повтор отдельной попыткой.
"""
import asyncio
import json
import socket
import threading
import time
from types import SimpleNamespace

import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse, StreamingResponse

from app.artifact import load_artifact
from app.backend import PrefixCache, prefix_keys
from app.router import choose
from loadgen import client as lc

ROOT_SHA_A = 'f5518680fa9a5e25637d47dedacab62f4d3dcf066bce34b7313efd705735b5b1'


def _fake_app():
    app = FastAPI()
    calls = {}

    @app.post('/v1/chat')
    async def chat(body: dict):
        sid = body['session_id']
        calls[sid] = calls.get(sid, 0) + 1
        n = body['max_chunks']

        async def stream(cut=None, stall=False):
            yield json.dumps(dict(type='meta', queue_s=0)) + '\n'
            for i in range(n):
                if cut is not None and i == cut:
                    raise RuntimeError('обрыв потока')  # uvicorn рвёт соединение посреди ответа
                if stall:
                    await asyncio.sleep(5)
                yield json.dumps(dict(type='chunk', i=i, text='x')) + '\n'
            yield json.dumps(dict(type='done', chunks=n)) + '\n'
        if sid == 'rej':
            return JSONResponse(status_code=429, content=dict(error='queue is full'))
        if sid == 'err':
            return JSONResponse(status_code=500, content=dict(error='boom'))
        if sid == 'cut':
            return StreamingResponse(stream(cut=3), media_type='application/x-ndjson')
        if sid == 'cut-once' and calls[sid] == 1:  # первая попытка оборвана, повтор успешен
            return StreamingResponse(stream(cut=2), media_type='application/x-ndjson')
        if sid == 'slow':
            return StreamingResponse(stream(stall=True), media_type='application/x-ndjson')
        return StreamingResponse(stream(), media_type='application/x-ndjson')
    return app


@pytest.fixture(scope='module')
def fake_server():
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    server = uvicorn.Server(uvicorn.Config(_fake_app(), host='127.0.0.1', port=port, log_level='error'))
    th = threading.Thread(target=server.run, daemon=True)
    th.start()
    while not server.started:
        time.sleep(0.05)
    yield f'http://127.0.0.1:{port}'
    server.should_exit = True


def test_4_failure_accounting(fake_server, tmp_path):
    sessions = ['ok', 'rej', 'err', 'cut', 'cut-once', 'slow']
    sched = dict(prefix_chars=50, turn_chars=20, requests=[
        dict(id=i, session=s, turn=0, client=0, offset_s=0.05 * i, phase='measure', max_chunks=6)
        for i, s in enumerate(sessions)])
    (tmp_path / 's.json').write_text(json.dumps(sched))
    args = SimpleNamespace(schedule=str(tmp_path / 's.json'), client=0, target=fake_server, start_at=0,
                           timeout=1.5, retries=1, max_connections=8, run='t', keep_text=False,
                           out=str(tmp_path / 'r.jsonl'), no_keepalive=False)
    asyncio.run(lc.run(args))
    recs = [json.loads(l) for l in (tmp_path / 'r.jsonl').read_text().splitlines()]
    by = {}
    for r in recs:
        by.setdefault(r['session'], []).append(r['outcome'])
    assert by['ok'] == ['ok']
    assert by['rej'] == ['rejected', 'rejected']          # 429 — отказ, повтор — отдельная попытка
    assert by['err'] == ['http_5xx', 'http_5xx']
    assert by['cut'] == ['aborted', 'aborted']            # 200 + поток без done — оборван, не успех
    assert by['cut-once'] == ['aborted', 'ok']            # повтор учтён отдельно, логически успешен
    assert by['slow'] == ['timeout', 'timeout']           # просрочен по тайм-ауту клиента
    from scripts.kexp import summarize
    s = summarize(recs)
    assert s['attempts'] == 11 and s['logical'] == 6 and s['logical_ok'] == 2 and s['ok'] == 2
    assert s['outcomes'] == {'ok': 2, 'rejected': 2, 'http_5xx': 2, 'aborted': 3, 'timeout': 2}
    assert s['n_ttfc'] == 2  # перцентили задержки — только по успешным


def test_router_affinity_and_overflow():
    eps = ['10.0.0.1', '10.0.0.2']
    first, why = choose('cache_aware', 's1', eps, {}, 2)
    assert why == 'affinity' and choose('cache_aware', 's1', eps, {}, 2)[0] == first
    other = [e for e in eps if e != first][0]
    assert choose('cache_aware', 's1', eps, {first: 3, other: 0}, 2) == (other, 'overflow')
    assert choose('cache_aware', 's1', eps, {first: 1, other: 0}, 2) == (first, 'affinity')
    assert choose('least_loaded', 's1', eps, {first: 1, other: 0}, 2)[0] == other


def test_prefix_cache_bounded_lru():
    c = PrefixCache(3)
    msgs = [dict(role='system', content='doc'), dict(role='user', content='q1')]
    keys = prefix_keys(msgs)
    assert c.longest(keys) == 0
    c.insert(keys)
    assert c.longest(keys) == len('docq1')
    c.insert(prefix_keys([dict(role='user', content=f'x{i}') for i in range(3)]))
    assert len(c) == 3 and c.longest(keys) == 0  # вытеснено при ограниченном размере


def test_artifact_hash_is_verified():
    assert load_artifact('models/model-A.json', ROOT_SHA_A, 'A')['version'] == 'A'
    with pytest.raises(ValueError, match='sha256'):
        load_artifact('models/model-A.json', '0' * 64, 'A')
