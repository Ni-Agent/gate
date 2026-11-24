"""Make a distinct chat address per Gate execution revision, stable on replay."""
import hashlib
import json


def task_event_route(origin: str, identity: str, task_id: str, revision: int) -> str:
    raw = json.dumps([origin.rstrip('/'), identity, task_id, revision], separators=(',', ':')).encode()
    return 'gate-v3-' + hashlib.sha256(raw).hexdigest()
