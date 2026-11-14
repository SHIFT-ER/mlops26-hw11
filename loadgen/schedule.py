"""Детерминированное расписание запросов (одно и то же для сравниваемых политик).

Пуассоновский поток с кусочно-постоянной интенсивностью --profile "сек:rps,сек:rps,...".
Каждое поступление — следующий ход случайного диалога. Диалог принадлежит одному клиенту:
  uniform — диалоги поровну между клиентами; skew80 — 80% запросов идут через клиента 0.
Тексты сообщений не хранятся: клиент восстанавливает их функцией build_messages по
(session, turn), длины входов задаются параметрами и одинаковы для всех вариантов.

  python -m loadgen.schedule --profile 15:4,120:4 --warmup 15 --traffic skew80 --out sched.json
"""
import argparse
import hashlib
import json
import random

WORDS = ('данные модель запрос ответ сервис очередь реплика кластер метрика прогрев выпуск версия '
         'пользователь диалог контекст префикс токен поток задержка нагрузка').split()


def _text(seed: str, chars: int) -> str:
    rnd = random.Random(int(hashlib.sha256(seed.encode()).hexdigest()[:12], 16))
    out, n = [], 0
    while n < chars:
        w = rnd.choice(WORDS)
        out.append(w)
        n += len(w) + 1
    return ' '.join(out)[:chars]


def build_messages(session: str, turn: int, prefix_chars: int, turn_chars: int, control: str | None = None) -> list:
    """История диалога передаётся в запросе: system-документ сессии + прошлые ходы + новый вопрос."""
    ctl = control or f'CTL-{session}'
    msgs = [dict(role='system', content=f'{ctl} ' + _text(f'{session}/doc', prefix_chars))]
    for t in range(turn):
        msgs.append(dict(role='user', content=_text(f'{session}/q{t}', turn_chars)))
        msgs.append(dict(role='assistant', content=_text(f'{session}/a{t}', turn_chars // 2)))
    msgs.append(dict(role='user', content=_text(f'{session}/q{turn}', turn_chars)))
    return msgs


def build(profile, warmup, sessions, clients, traffic, chunks, prefix_chars, turn_chars, seed, long_streams=(),
          max_turns=6):
    rnd = random.Random(seed)
    hot = round(sessions * 0.8)
    owner = {}
    for s in range(sessions):
        if traffic == 'skew80':
            owner[s] = 0 if s < hot else 1 + (s - hot) % (clients - 1)
        else:
            owner[s] = s % clients
    turns = [0] * sessions
    reqs, t, start = [], 0.0, 0.0
    for dur, rate in profile:
        end = start + dur
        t = max(t, start)
        while rate > 0:
            t += rnd.expovariate(rate)
            if t >= end:
                break
            s = rnd.randrange(sessions)
            # диалог длится max_turns ходов, затем у того же пользователя начинается новый (новый префикс):
            # длина входа ограничена, нагрузка стационарна на всей фазе измерения
            epoch, turn = divmod(turns[s], max_turns)
            reqs.append(dict(id=len(reqs), session=f's{s:02d}e{epoch}', turn=turn, client=owner[s],
                             offset_s=round(t, 4), phase='warmup' if t < warmup else 'measure',
                             max_chunks=rnd.randint(*chunks)))
            turns[s] += 1
        start = end
    for i, (offset, n, client) in enumerate(long_streams):  # длительные потоковые ответы
        reqs.append(dict(id=len(reqs), session=f'long{i}', turn=0, client=client, offset_s=offset,
                         phase='measure', max_chunks=n, long=True))
    reqs.sort(key=lambda r: r['offset_s'])
    return dict(seed=seed, traffic=traffic, profile=profile, warmup_s=warmup, sessions=sessions, max_turns=max_turns,
                clients=clients, chunks=list(chunks), prefix_chars=prefix_chars, turn_chars=turn_chars,
                duration_s=sum(d for d, _ in profile), requests=reqs)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--profile', default='15:4,120:4', help='сек:rps через запятую')
    p.add_argument('--warmup', type=float, default=15)
    p.add_argument('--sessions', type=int, default=20)
    p.add_argument('--clients', type=int, default=4)
    p.add_argument('--traffic', choices=['uniform', 'skew80'], default='uniform')
    p.add_argument('--chunks', default='16,32')
    p.add_argument('--prefix-chars', type=int, default=2400)
    p.add_argument('--turn-chars', type=int, default=160)
    p.add_argument('--seed', type=int, default=11)
    p.add_argument('--max-turns', type=int, default=6)
    p.add_argument('--long', action='append', default=[], help='offset:chunks:client — длительный поток')
    p.add_argument('--out', required=True)
    a = p.parse_args()
    profile = [(float(d), float(r)) for d, r in (x.split(':') for x in a.profile.split(','))]
    longs = [(float(o), int(n), int(c)) for o, n, c in (x.split(':') for x in a.long)]
    sched = build(profile, a.warmup, a.sessions, a.clients, a.traffic, tuple(map(int, a.chunks.split(','))),
                  a.prefix_chars, a.turn_chars, a.seed, longs, a.max_turns)
    with open(a.out, 'w') as f:
        json.dump(sched, f, ensure_ascii=False)
    by = {}
    for r in sched['requests']:
        by[r['client']] = by.get(r['client'], 0) + 1
    print(json.dumps(dict(out=a.out, requests=len(sched['requests']), per_client=by)))


if __name__ == '__main__':
    main()
