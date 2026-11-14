"""Клиент нагрузки: исполняет свою часть расписания и пишет сырой результат каждой попытки (JSONL).

Время: моменты отправки привязаны к общему start_at (wall clock узла), длительности — к
монотонным часам клиента (time.monotonic). В записи есть и запланированное, и фактическое
время отправки: send_lag_s показывает отставание самого генератора нагрузки.

  python -m loadgen.client --schedule s.json --client 0 --target http://llm-sim:8000 \
      --start-at 1731600000 --out /tmp/r.jsonl [--retries 1]
"""
import argparse
import asyncio
import json
import os
import socket
import time
import uuid

import httpx

from .schedule import build_messages

TERMINAL_OK = 'ok'


def classify(status: int | None, got_done: bool, chunks: int, expected: int, error: str | None) -> str:
    """Исход попытки. Чистая функция — покрыта собственной проверкой учёта отказов."""
    if error == 'timeout':
        return 'timeout'
    if status is None:
        return 'conn_error'
    if status == 429:
        return 'rejected'
    if status >= 500:
        return 'http_5xx'
    if status >= 400:
        return 'http_4xx'
    if not got_done:
        return 'aborted'  # ответ начат (200), но поток оборван до строки done
    if chunks != expected:
        return 'incomplete'
    return TERMINAL_OK


async def one(client, args, sched, req, start_mono, start_wall, attempt):
    msgs = build_messages(req['session'], req['turn'], sched['prefix_chars'], sched['turn_chars'],
                          req.get('control'))
    rid = f"{args.run}-{req['id']}-a{attempt}" if args.run else str(uuid.uuid4())
    rec = dict(run=args.run, id=req['id'], attempt=attempt, request_id=rid, session=req['session'],
               turn=req['turn'], phase=req['phase'], client=req['client'], client_pod=socket.gethostname(),
               max_chunks=req['max_chunks'], prompt_chars=sum(len(m['content']) for m in msgs),
               scheduled_wall=start_wall + req['offset_s'], long=req.get('long', False))
    sent = time.monotonic()
    rec['send_wall'] = time.time()
    rec['send_lag_s'] = round(sent - (start_mono + req['offset_s']), 4)
    status, done, chunks, error, first, meta, text = None, False, 0, None, None, {}, []
    try:
        async with asyncio.timeout(args.timeout):
            hdr = {'x-request-id': rid}
            if args.no_keepalive:
                hdr['connection'] = 'close'
            async with client.stream('POST', args.target + '/v1/chat', headers=hdr,
                                     json=dict(session_id=req['session'], messages=msgs,
                                               max_chunks=req['max_chunks'], stream=True)) as r:
                status = r.status_code
                rec.update(pod=r.headers.get('x-pod'), model_version=r.headers.get('x-model-version'),
                           route_reason=r.headers.get('x-route-reason'))
                rec['headers_s'] = round(time.monotonic() - sent, 4)
                if status != 200:
                    body = await r.aread()
                    try:
                        error = json.loads(body).get('error')
                    except ValueError:
                        error = body[:200].decode(errors='replace')
                else:
                    async for line in r.aiter_lines():
                        if not line:
                            continue
                        item = json.loads(line)
                        if item['type'] == 'meta':
                            meta = item
                        elif item['type'] == 'chunk':
                            if first is None:
                                first = time.monotonic()
                            chunks += 1
                            text.append(item['text'])
                        elif item['type'] == 'done':
                            done = True
                        elif item['type'] == 'error':
                            error = item.get('error')
    except TimeoutError:
        error = 'timeout'
    except httpx.HTTPError as exc:
        error = f'{type(exc).__name__}: {exc}'[:200]
    end = time.monotonic()
    rec.update(status=status, chunks=chunks, got_done=done, error=error,
               ttfc_s=None if first is None else round(first - sent, 4),
               duration_s=round(end - sent, 4), done_wall=time.time(),
               queue_s=meta.get('queue_s'), cache=meta.get('cache'), cached_chars=meta.get('cached_chars'),
               warm_at_receive=meta.get('warm_at_receive'), model_sha256=meta.get('model_sha256'),
               backend_mode=meta.get('backend_mode'))
    rec['inter_chunk_mean_s'] = (round((end - first) / (chunks - 1), 5) if first and chunks > 1 and done else None)
    rec['outcome'] = classify(status, done, chunks, req['max_chunks'], error)
    if args.keep_text:
        rec['text'] = ''.join(text)
    return rec


async def run(args):
    with open(args.schedule) as f:
        sched = json.load(f)
    mine = [r for r in sched['requests'] if r['client'] == args.client]
    out = open(args.out, 'w')
    # --no-keepalive: соединение на каждый запрос (как за L7-балансировщиком, который распределяет КАЖДЫЙ
    # запрос); иначе kube-proxy выбирает Pod только для нового TCP-соединения
    keep = 0 if args.no_keepalive else args.max_connections
    limits = httpx.Limits(max_connections=args.max_connections, max_keepalive_connections=keep)
    async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(args.timeout, connect=3)) as client:
        now = time.time()
        if args.start_at > now:
            await asyncio.sleep(args.start_at - now)
        start_mono, start_wall = time.monotonic(), args.start_at or time.time()

        async def task(req):
            delay = start_mono + req['offset_s'] - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            for attempt in range(args.retries + 1):
                rec = await one(client, args, sched, req, start_mono, start_wall, attempt)
                out.write(json.dumps(rec, ensure_ascii=False) + '\n')
                out.flush()
                if rec['outcome'] in (TERMINAL_OK, 'http_4xx'):
                    break

        await asyncio.gather(*(task(r) for r in mine))
    out.close()
    print(json.dumps(dict(client=args.client, pod=socket.gethostname(), planned=len(mine), out=args.out)))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--schedule', required=True)
    p.add_argument('--client', type=int, default=0)
    p.add_argument('--target', default=os.environ.get('TARGET', 'http://llm-sim:8000'))
    p.add_argument('--start-at', type=float, default=0)
    p.add_argument('--timeout', type=float, default=30, help='предельное время попытки, с')
    p.add_argument('--retries', type=int, default=0, help='клиентские повторы (каждый — отдельная попытка)')
    p.add_argument('--max-connections', type=int, default=32)
    p.add_argument('--run', default='')
    p.add_argument('--keep-text', action='store_true')
    p.add_argument('--no-keepalive', action='store_true')
    p.add_argument('--out', required=True)
    asyncio.run(run(p.parse_args()))


if __name__ == '__main__':
    main()
