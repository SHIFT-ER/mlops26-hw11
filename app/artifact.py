"""Проверка версии артефакта (перенесено из release.verify_files ДЗ 8+9+10).

Источник артефакта на CPU-стенде — файл модели-имитатора внутри неизменяемого образа
(models/model-<версия>.txt). Ожидаемый sha256 задаётся в Helm values -> ConfigMap,
поэтому Pod после замены пересчитывает хеш и загружает ту же версию или не становится Ready.
"""
import hashlib
import json
from pathlib import Path


def sha256_file(path) -> str:
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def load_artifact(path, expected_sha256: str, expected_version: str) -> dict:
    """Пересчитать хеш файла, сверить с закреплённым и вернуть параметры имитатора."""
    path = Path(path)
    if not path.is_file():
        raise ValueError(f'нет файла артефакта {path}')
    actual = sha256_file(path)
    if actual != expected_sha256:
        raise ValueError(f'sha256 артефакта {path.name} = {actual[:12]}…, ожидался {expected_sha256[:12]}…')
    meta = json.loads(path.read_text(encoding='utf-8'))
    if meta.get('version') != expected_version:
        raise ValueError(f'в артефакте версия {meta.get("version")}, ожидалась {expected_version}')
    meta['sha256'] = actual
    return meta
