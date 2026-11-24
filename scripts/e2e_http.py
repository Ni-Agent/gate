#!/usr/bin/env python3
"""Bounded synthetic Go socket acceptance; no provider or live Gate access."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from client.gate_client import GateClient, GateError


def run(actor='agent:hermes'):
    with tempfile.TemporaryDirectory(prefix='gate-http-e2e-', dir=os.environ['TMPDIR']) as tmp:
        tmp = Path(tmp)
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        origin = f'http://127.0.0.1:{port}'
        process = subprocess.Popen(
            [str(ROOT / '.bin/gate-alpha'), '--store', str(tmp / 'store'), '--http', f'127.0.0.1:{port}',
             '--actor', actor, '--identities', actor],
            env={'PATH': os.environ.get('PATH', ''), 'TMPDIR': str(tmp),
                 'GATE_ALPHA_TEST_TOKEN': 'synthetic-loopback-test-token'},
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        client = GateClient(origin, 'synthetic-loopback-test-token', timeout=5, allow_loopback_http=True)
        requests, upload_chunks = [], []
        transport_request = httpx.HTTPTransport.handle_request

        class ObservedStream(httpx.SyncByteStream):
            def __init__(self, stream):
                self.stream = stream

            def __iter__(self):
                for chunk in self.stream:
                    upload_chunks.append(len(chunk))
                    yield chunk

        def observe(transport, request):
            requests.append((request.method, request.url.path))
            if request.url.path == '/binary' and request.method in ('POST', 'PUT'):
                request.stream = ObservedStream(request.stream)
            return transport_request(transport, request)

        mutations = []

        def committed(operation, **arguments):
            before = len(requests)
            value = client.rpc(operation, **arguments)
            # Count actual transport invocations, not mocked rpc or readback.
            assert requests[before:] == [('POST', '/rpc/' + operation)]
            assert value.get('id') and value.get('token')
            mutations.append(operation)
            return value

        def pre(resource):
            return dict(id=resource['id'], expected_revision=resource['revision'], expected_hash=resource['token'])

        def expect(status, call):
            before = len(requests)
            try:
                call()
            except GateError as error:
                assert error.status == status, (error.status, error.payload)
                assert isinstance(error.payload, dict) and error.payload.get('error')
                assert len(requests) == before + 1, 'automatic retry/recheck on error'
            else:
                raise AssertionError(f'expected server HTTP {status}')

        try:
            deadline = time.monotonic() + 8
            while True:
                try:
                    client.authenticate(actor)
                    break
                except GateError:
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise
                    time.sleep(.05)
            with patch.object(httpx.HTTPTransport, 'handle_request', observe):
                wrong_auth = GateClient(origin, 'synthetic-wrong-token', allow_loopback_http=True)
                expect(401, lambda: wrong_auth.authenticate())
                tag = committed('create_tag', id='gate:tg:project', key='project', value='E2E')
                text = committed('create_text', id='gate:tx:flow', body='one', tags=[tag['id']])
                stale = dict(text)
                text = committed('update_text', **pre(text), body='two')
                assert text['body'] == 'two'
                expect(412, lambda: client.rpc('update_text', **pre(stale), body='stale'))
                expect(400, lambda: client.rpc('update_text', id=text['id'], body='missing fields'))
                expect(428, lambda: client.rpc('update_text', id=text['id'], expected_revision=1,
                                               expected_hash='', body='missing validator'))
                expect(400, lambda: client.rpc('read_resource', id='gate:tx:Invalid'))
                text = committed('seal_revision', **pre(text))
                assert text['revision'] == 2
                assert client.rpc('read_revision', id=text['id'], revision=1)['body'] == 'two'
                tomb = committed('delete_resource', **pre(text))
                expect(410, lambda: client.rpc('read_resource', id=text['id']))
                text = committed('update_text', **pre(tomb), body='restored')
                assert text['body'] == 'restored'

                task = committed('create_task', operation_id='socket-task', assignee=actor, body='initial')
                old_token = task['token']
                task = committed('update_task', id=task['id'], expected_token=old_token,
                                 status='ready', body='assigned + ready', add_session='socket:session:one')
                assert task['status'] == 'ready' and task['body'] == 'assigned + ready'
                expect(412, lambda: client.rpc('update_task', id=task['id'], expected_token=old_token, body='stale'))
                expect(428, lambda: client.rpc('update_task', id=task['id'], expected_token='', body='missing token'))
                assert client.rpc('list_tasks', assignee=actor)['items'][0]['token'] == task['token']
                task = committed('update_task', id=task['id'], expected_token=task['token'], status='done')
                assert task['status'] == 'done'

                source, target, historical = tmp / 'source', tmp / 'target', tmp / 'historical'
                with source.open('wb') as stream:
                    for _ in range(192):
                        stream.write(bytes(range(256)) * 256)
                with source.open('rb') as stream:
                    original_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
                binary = client.upload_binary('gate:bn:disk-stream', source, tags=[tag['id']])
                assert binary['token'] == original_hash
                metadata = client.download_binary_file(binary['id'], target)
                with target.open('rb') as stream:
                    assert hashlib.file_digest(stream, 'sha256').hexdigest() == original_hash
                assert metadata['token'] == original_hash and target.stat().st_size == 12 << 20
                expect(400, lambda: client.upload_binary('gate:tx:wrong-type', source))
                # A partial update precondition must reach the server's 428.
                expect(428, lambda: client.upload_binary(binary['id'], source, expected_revision=1))
                binary = committed('seal_revision', **pre(binary))
                with source.open('r+b') as stream:
                    stream.write(b'changed bytes\x00\xff')
                with source.open('rb') as stream:
                    updated_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
                current = client.upload_binary(binary['id'], source, expected_revision=binary['revision'], expected_hash=binary['token'])
                assert current['token'] == updated_hash and updated_hash != original_hash
                expect(412, lambda: client.upload_binary(binary['id'], source, expected_revision=binary['revision'], expected_hash=binary['token']))
                client.download_binary_file(binary['id'], target)
                client.download_binary_file(binary['id'], historical, revision=1)
                for path, digest in ((target, updated_hash), (historical, original_hash)):
                    with path.open('rb') as stream:
                        assert hashlib.file_digest(stream, 'sha256').hexdigest() == digest
                    assert path.stat().st_size == 12 << 20
                committed('delete_resource', **pre(current))
                expect(410, lambda: client.download_binary_file(binary['id'], target))
                assert not list(tmp.glob('.gate-download-*'))
                assert upload_chunks and max(upload_chunks) <= 65536, 'upload was buffered'
                # One more ordinary generic update checks opaque committed metadata.
                text = committed('update_text', **pre(text), body='final')
                assert text['body'] == 'final'
                assert len(mutations) == 12
                return {'result': 'PASS', 'transport': 'httpx over actual loopback Go HTTP sockets',
                        'identity': actor, 'one_request_mutations': len(mutations),
                        'automatic_readback_or_auth_recheck': False,
                        'text_task': 'generic committed-200/CAS/seal/history/delete/restore/task updates',
                        'server_statuses': [401, 400, 412, 428, 410],
                        'binary': {'bytes': target.stat().st_size, 'original_sha256': original_hash,
                                   'updated_sha256': updated_hash, 'maximum_observed_upload_chunk': max(upload_chunks),
                                   'flows': 'disk create/download/seal/update/latest+history/stale/delete; atomic target preservation'},
                        'request_count': len(requests), 'cleanup': 'server terminated/reaped; private directory removed'}
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            if process.stderr is not None:
                process.stderr.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--actor', default='agent:hermes')
    print(json.dumps(run(parser.parse_args().actor), indent=2))
