"""Проверки, обращающиеся к сервису в Kubernetes (нужен поднятый стенд: scripts/up.sh).

  uv run pytest tests/test_k8s.py -v
Проверка прогрева на исходном дефекте (values-naive-probes.yaml) ПАДАЕТ — это предъявление сбоя.
"""
import time

import pytest

from kube import (backend_pods, chat, configured_model, curl_code, kubectl, ready_endpoints,
                  wait_for)
from loadgen.schedule import build_messages

pytestmark = pytest.mark.k8s


def _msgs(session, turn, ctl):
    return build_messages(session, turn, 600, 80, control=ctl)


def test_1_warming_pod_gets_no_traffic():
    """Пока /startup нового Pod != 200, Pod не Ready и отсутствует в EndpointSlice; после — появляется."""
    version, sha, warmup = configured_model()
    old = {p['metadata']['name'] for p in backend_pods()}
    victim = sorted(old)[0]
    kubectl('delete', 'pod', victim, '--wait=false')
    new = wait_for(lambda: [p for p in backend_pods() if p['metadata']['name'] not in old
                            and p['status'].get('podIP')], timeout=60)[0]
    name, ip = new['metadata']['name'], new['status']['podIP']
    observed_warming = 0
    t0 = time.time()
    while time.time() - t0 < warmup + 40:
        code = curl_code(f'http://{ip}:8000/startup')
        in_eps = name in ready_endpoints()
        if code != '200':
            observed_warming += 1
            assert not in_eps, f'{name} прогревается (/startup={code}), но уже в Ready EndpointSlice'
        elif in_eps:
            break
        time.sleep(1)
    assert observed_warming >= 3, 'не удалось наблюдать фазу прогрева'
    assert name in ready_endpoints(), 'прогретый Pod так и не стал Ready'
    logs = kubectl('logs', name)
    assert '"warm_at_receive": false' not in logs, 'непрогретый backend получил рабочий запрос'


def test_2_loaded_version_matches_release():
    """Фактические ответы через Service несут версию и sha256 из текущего релиза Helm (после отката)."""
    version, sha, _ = configured_model()
    seen = set()
    for i in range(12):
        r = chat(f'ver-{i}', _msgs(f'ver-{i}', 0, f'CTL-ver{i}'), max_chunks=2)
        assert r['status'] == 200, r
        assert r['backend_mode'] == 'simulated'
        seen.add((r['model_version'], r['model_sha256'], r['pod']))
    assert {(v, s) for v, s, _ in seen} == {(version, sha)}, seen
    assert len({p for *_, p in seen}) == 2, f'ответили не обе реплики: {seen}'


def test_3_dialog_survives_pod_replacement():
    """Ход 1 -> удалить Pod, ответивший на него -> ход 2 с историей в запросе отвечает другая реплика."""
    version, sha, _ = configured_model()
    ctl = 'CTL-recover'
    r1 = chat('recover', _msgs('recover', 0, ctl), url='http://llm-sim-clientip:8000')
    assert r1['status'] == 200 and ctl in r1['text']
    kubectl('delete', 'pod', r1['pod'], '--wait=false')
    wait_for(lambda: r1['pod'] not in ready_endpoints('llm-sim-clientip'), timeout=30, period=0.5)
    r2 = chat('recover', _msgs('recover', 1, ctl), url='http://llm-sim-clientip:8000')
    assert r2['status'] == 200, r2
    assert r2['pod'] != r1['pod'] and ctl in r2['text']
    assert (r2['model_version'], r2['model_sha256']) == (version, sha)
    # новый Pod-замена поднимает ту же версию: дождаться 2 Ready и проверить sha на нём
    wait_for(lambda: len(ready_endpoints()) == 2, timeout=120)
    for p in backend_pods():
        out = kubectl('exec', 'loadclient-3', '--', 'curl', '-s', f'http://{p["status"]["podIP"]}:8000/version')
        assert sha in out and f'"model_version":"{version}"' in out, out


@pytest.mark.parametrize('url', ['http://llm-sim:8000', 'http://llm-router:8000'])
def test_5_two_dialogs_are_isolated(url):
    """Два диалога с разными контрольными строками чередуются через одну точку входа и не смешиваются."""
    for turn in range(3):
        ra = chat('iso-alpha', _msgs('iso-alpha', turn, 'CTL-ALPHA-7Q'), url=url)
        rb = chat('iso-bravo', _msgs('iso-bravo', turn, 'CTL-BRAVO-3Z'), url=url)
        assert 'CTL-ALPHA-7Q' in ra['text'] and 'CTL-BRAVO-3Z' not in ra['text'], ra
        assert 'CTL-BRAVO-3Z' in rb['text'] and 'CTL-ALPHA-7Q' not in rb['text'], rb
        assert ra['session_id'] == 'iso-alpha' and rb['session_id'] == 'iso-bravo'
