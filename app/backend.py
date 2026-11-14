"""Управляемый CPU-backend: имитатор модели с прогревом и локальным префиксным кешем.

Интерфейс тот же, что у backend ДЗ 8+9+10: load(), warmup(), generate(). Сервис не знает,
какой backend внедрён. Здесь он один — SimulatedBackend, kind = backend_mode = 'simulated'.

Модель нагрузки (все числа — параметры имитатора, а не свойства настоящего inference engine):
  * prefill: CPU-работа PREFILL_MS_PER_KCHAR на каждую 1000 символов НЕ найденного в кеше
    префикса диалога + PREFILL_BASE_MS;
  * локальный префиксный кеш: LRU на CACHE_ENTRIES ключей; ключ — sha256 префикса сообщений
    на каждой границе сообщения (аналог блочного prefix caching);
  * decode: каждая часть ответа — CHUNK_CPU_MS CPU-времени потока.
CPU-время меряется time.thread_time(), поэтому при конкуренции за ядро (GIL, CPU limit)
работа растягивается во времени, а загрузка CPU видна метрикам и HPA.
"""
import hashlib
import threading
import time
from collections import OrderedDict

from .artifact import load_artifact


def burn_cpu(ms: float) -> None:
    """Потратить ms миллисекунд CPU-времени текущего потока (вычислительная нагрузка)."""
    if ms <= 0:
        return
    end = time.thread_time() + ms / 1000.0
    x = 0
    while time.thread_time() < end:
        for i in range(2000):
            x = (x * 1103515245 + i) & 0xFFFFFFFF


def prefix_keys(messages: list[dict]) -> list[tuple[str, int]]:
    """Ключи префикса на каждой границе сообщения: [(sha, длина префикса в символах), ...]."""
    h = hashlib.sha256()
    keys, length = [], 0
    for m in messages:
        part = f"{m['role']}\x1f{m['content']}\x1e".encode()
        h.update(part)
        length += len(m['content'])
        keys.append((h.copy().hexdigest()[:16], length))
    return keys


class PrefixCache:
    """LRU ограниченного размера. Потокобезопасный: обращаются потоки обработки запросов."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._data: OrderedDict[str, int] = OrderedDict()
        self._lock = threading.Lock()

    def longest(self, keys: list[tuple[str, int]]) -> int:
        """Длина (в символах) самого длинного закешированного префикса; 0 — промах."""
        with self._lock:
            for key, length in reversed(keys):
                if key in self._data:
                    self._data.move_to_end(key)
                    return length
        return 0

    def insert(self, keys: list[tuple[str, int]]) -> None:
        with self._lock:
            for key, length in keys:
                self._data[key] = length
                self._data.move_to_end(key)
            while len(self._data) > self.capacity:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self):
        return len(self._data)


class SimulatedBackend:
    kind = 'simulated'

    def __init__(self, cfg):
        self.cfg = cfg
        self.cache = PrefixCache(cfg.cache_entries)
        self.meta = None
        self.version = None
        self.sha256 = None

    def load(self):
        self.meta = load_artifact(self.cfg.model_file, self.cfg.model_sha256, self.cfg.model_version)
        self.version, self.sha256 = self.meta['version'], self.meta['sha256']

    def warmup(self, seconds: float, stop: threading.Event):
        """Прогрев: заметная пауза + немного CPU (аналог компиляции/загрузки весов)."""
        started = time.monotonic()
        burn_cpu(min(200.0, seconds * 1000 / 10))
        while time.monotonic() - started < seconds and not stop.is_set():
            time.sleep(0.1)

    def prefill(self, messages: list[dict]) -> dict:
        keys = prefix_keys(messages)
        total = keys[-1][1]
        cached = self.cache.longest(keys) if self.cfg.cache_enabled else 0
        miss_chars = total - cached
        cost = self.cfg.prefill_base_ms + self.cfg.prefill_ms_per_kchar * miss_chars / 1000.0
        burn_cpu(cost)
        if self.cfg.cache_enabled:
            self.cache.insert(keys)
        result = 'hit' if cached else 'miss'
        return dict(cache=result, cached_chars=cached, prompt_chars=total, prefill_cpu_ms=round(cost, 1))

    def chunks(self, messages: list[dict], n: int):
        """Ответ частями. Текст детерминирован: версия модели + контрольная строка диалога."""
        words = self.meta['vocabulary']
        last_user = next((m['content'] for m in reversed(messages) if m['role'] == 'user'), '')
        seed = int(hashlib.sha256(last_user.encode()).hexdigest()[:8], 16)
        control = _control_token(messages)
        for i in range(n):
            burn_cpu(self.cfg.chunk_cpu_ms)
            if i == 0:
                text = f'[{self.meta["reply_prefix"]} {self.version}] ctl={control} '
            else:
                text = words[(seed + i * 7) % len(words)] + ' '
            yield text


def _control_token(messages: list[dict]) -> str:
    """Контрольная строка диалога: первое слово вида CTL-... в истории (для проверки изоляции)."""
    for m in messages:
        for w in m['content'].split():
            if w.startswith('CTL-'):
                return w
    return '-'
