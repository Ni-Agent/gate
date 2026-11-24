"""Stable native route encoding shared by ingress and task tools."""
import base64
import json


def canonical(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False)


def task_route(origin: str, identity: str, task_id: str) -> str:
    # Reversible origin/account/task tuple, not an execution-state cache.
    raw=canonical([origin.rstrip('/'),identity,task_id]).encode()
    return 'gate-v2-'+base64.urlsafe_b64encode(raw).decode().rstrip('=')
