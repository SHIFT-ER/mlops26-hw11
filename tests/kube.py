"""Помощники проверок: запросы к сервису ИЗНУТРИ кластера (из клиентского Pod) через kubectl exec."""
import json
import subprocess
import time

NS = 'llm'
CLIENT = 'loadclient-3'

_CHAT = r'''
import json, sys, urllib.request
a = json.loads(sys.argv[1])
req = urllib.request.Request(a["url"] + "/v1/chat", data=json.dumps(a["body"]).encode(),
                             headers={"content-type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=a.get("timeout", 60)) as r:
        print(json.dumps(dict(status=r.status, **json.loads(r.read()))))
except urllib.error.HTTPError as e:
    print(json.dumps(dict(status=e.code, **json.loads(e.read() or b"{}"))))
'''


def kubectl(*args, input=None, check=True):
    r = subprocess.run(['kubectl', '-n', NS, *args], capture_output=True, text=True, input=input)
    if check and r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout


def kget(*args):
    return json.loads(kubectl('get', *args, '-o', 'json'))


def chat(session, messages, url='http://llm-sim:8000', max_chunks=8, client=CLIENT):
    body = dict(session_id=session, messages=messages, max_chunks=max_chunks, stream=False)
    out = kubectl('exec', '-i', client, '--', 'python', '-', json.dumps(dict(url=url, body=body)), input=_CHAT)
    return json.loads(out.strip().splitlines()[-1])


def curl_code(url, client=CLIENT):
    return kubectl('exec', client, '--', 'curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', '-m', '2',
                   url, check=False).strip()


def backend_pods():
    return [p for p in kget('pods', '-l', 'app.kubernetes.io/component=backend')['items']
            if not p['metadata'].get('deletionTimestamp')]


def ready_endpoints(service='llm-sim'):
    names = set()
    for sl in kget('endpointslices', '-l', f'kubernetes.io/service-name={service}')['items']:
        for ep in sl.get('endpoints') or []:
            if ep['conditions'].get('ready'):
                names.add(ep['targetRef']['name'])
    return names


def wait_for(pred, timeout=120, period=1.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = pred()
        if v:
            return v
        time.sleep(period)
    raise TimeoutError('condition not met')


def configured_model():
    cm = kget('configmap', 'llm-sim-config')['data']
    return cm['MODEL_VERSION'], cm['MODEL_SHA256'], float(cm['WARMUP_SECONDS'])
