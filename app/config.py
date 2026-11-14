"""Параметры сервиса из окружения (в Kubernetes — из ConfigMap, который рендерит Helm)."""
import os
from dataclasses import dataclass, field


def _env(name, default, cast=str):
    value = os.environ.get(name)
    return default if value in (None, '') else cast(value)


def _bool(value) -> bool:
    return str(value).lower() in ('1', 'true', 'yes', 'on')


@dataclass
class Config:
    pod_name: str = field(default_factory=lambda: _env('POD_NAME', os.uname().nodename))
    pod_ip: str = field(default_factory=lambda: _env('POD_IP', '127.0.0.1'))
    model_version: str = field(default_factory=lambda: _env('MODEL_VERSION', 'A'))
    model_file: str = field(default_factory=lambda: _env('MODEL_FILE', 'models/model-A.json'))
    model_sha256: str = field(default_factory=lambda: _env('MODEL_SHA256', ''))
    warmup_seconds: float = field(default_factory=lambda: _env('WARMUP_SECONDS', 20.0, float))
    max_concurrency: int = field(default_factory=lambda: _env('MAX_CONCURRENCY', 2, int))
    max_waiting: int = field(default_factory=lambda: _env('MAX_WAITING', 16, int))
    cache_enabled: bool = field(default_factory=lambda: _env('CACHE_ENABLED', True, _bool))
    cache_entries: int = field(default_factory=lambda: _env('CACHE_ENTRIES', 256, int))
    prefill_base_ms: float = field(default_factory=lambda: _env('PREFILL_BASE_MS', 10.0, float))
    prefill_ms_per_kchar: float = field(default_factory=lambda: _env('PREFILL_MS_PER_KCHAR', 300.0, float))
    chunk_cpu_ms: float = field(default_factory=lambda: _env('CHUNK_CPU_MS', 10.0, float))
    max_chunks: int = field(default_factory=lambda: _env('MAX_CHUNKS', 400, int))
    warm_wait_timeout: float = field(default_factory=lambda: _env('WARM_WAIT_TIMEOUT', 60.0, float))
    drain_seconds: float = field(default_factory=lambda: _env('DRAIN_SECONDS', 5.0, float))
    graceful_seconds: int = field(default_factory=lambda: _env('GRACEFUL_SECONDS', 50, int))
    liveness_stall_seconds: float = field(default_factory=lambda: _env('LIVENESS_STALL_SECONDS', 60.0, float))
    port: int = field(default_factory=lambda: _env('PORT', 8000, int))
