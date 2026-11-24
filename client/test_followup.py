"""Faults observable at real HTTP sockets, not fake response internals."""
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from client.gate_client import GateClient, GateError


@dataclass
class FaultState:
    mode: str = 'whoami'
    paths: list = field(default_factory=list)
    committed: bool = False
    identity: str = 'hermes:default'
    protocol: str = 'gate-alpha-native-1'


@contextmanager
def fault_server():
    state = FaultState()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # Never log credentials or request data.

        def do_POST(self):
            state.paths.append(self.path)
            self.rfile.read(int(self.headers.get('Content-Length', 0)))
            # Model a committed mutation whose response is lost.
            state.committed = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()

        def do_GET(self):
            state.paths.append(self.path)
            mode = state.mode
            if mode == 'error-truncated':
                self.send_response(412)
                self.send_header('Content-Length', '100')
                self.end_headers()
                self.wfile.write(b'{"error":')
                self.wfile.flush()
                self.close_connection = True
                return
            if mode == 'redirect':
                self.send_response(307)
                self.send_header('Location', '/credential-sink')
                self.end_headers()
                return
            if mode == 'whoami':
                body = json.dumps({'protocol': state.protocol, 'identity': state.identity}).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            body = b'bad bytes' if mode == 'corrupt' else b'original bytes'
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Length', str(len(body) + (10 if mode == 'truncated' else 0)))
            self.send_header('X-Gate-ID', 'gate:bn:other' if mode == 'wrong-id' else 'gate:bn:fault')
            self.send_header('X-Gate-Revision', '1')
            self.send_header('X-Gate-Mime', 'application/octet-stream')
            self.send_header('ETag', '"' + hashlib.sha256(b'original bytes').hexdigest() + '"')
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            self.close_connection = True

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
    thread.start()
    client = GateClient(f'http://127.0.0.1:{server.server_port}', 'synthetic-fault-token', timeout=2,
                        allow_loopback_http=True)
    try:
        yield client, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()


class RealSocketFaultTests(unittest.TestCase):
    def test_known_server_status_survives_unreadable_error_body(self):
        with fault_server() as (client, state):
            state.mode = 'error-truncated'
            with self.assertRaises(GateError) as response:
                client.authenticate()
            self.assertEqual(response.exception.status, 412)
            self.assertIsInstance(response.exception.payload, dict)
            self.assertEqual(state.paths, ['/whoami'])

    def test_server_identity_authority_and_exact_account_pin(self):
        with fault_server() as (client, server):
            for identity in ('hermes:default', 'hermes:prometheus', 'agent:hermes', 'human:d0f'):
                server.identity = identity
                self.assertEqual(client.authenticate(identity), identity)
                with self.assertRaises(GateError) as wrong:
                    client.authenticate('hermes:different')
                self.assertEqual(wrong.exception.status, 403)
            server.protocol = 'not-gate'
            with self.assertRaises(GateError) as wrong_service:
                client.authenticate()
            self.assertEqual(wrong_service.exception.status, 403)

    def test_redirects_and_environment_proxies_cannot_forward_credentials(self):
        with fault_server() as (client, server), patch.dict(os.environ, {
                'HTTP_PROXY': 'http://127.0.0.1:1', 'HTTPS_PROXY': 'http://127.0.0.1:1',
                'ALL_PROXY': 'http://127.0.0.1:1', 'NO_PROXY': ''}):
            self.assertEqual(client.authenticate(), 'hermes:default')
            server.mode = 'redirect'
            with self.assertRaises(GateError) as redirect:
                client.authenticate()
            self.assertEqual(redirect.exception.status, 307)
            self.assertEqual(server.paths, ['/whoami', '/whoami'])
            self.assertNotIn(client.token, repr(redirect.exception))
            self.assertNotIn(client.token, str(redirect.exception))

    def test_lost_committed_response_is_unknown_and_never_retried(self):
        with fault_server() as (client, server):
            with self.assertRaises(GateError) as lost:
                client.rpc('create_text', id='gate:tx:lost', body='committed')
            self.assertEqual(lost.exception.status, 0)
            self.assertIn('outcome unknown', str(lost.exception))
            self.assertIn('outcome unknown', lost.exception.payload['error'])
            self.assertTrue(server.committed)
            self.assertEqual(server.paths, ['/rpc/create_text'])

    def test_failed_downloads_preserve_target_and_remove_staging(self):
        with fault_server() as (client, server), tempfile.TemporaryDirectory(dir=os.environ['TMPDIR']) as tmp:
            target = Path(tmp) / 'target'
            target.write_bytes(b'existing target')
            for mode, status in (('corrupt', 502), ('truncated', 0), ('wrong-id', 502)):
                server.mode = mode
                with self.subTest(mode=mode), self.assertRaises(GateError) as failure:
                    client.download_binary_file('gate:bn:fault', target)
                self.assertEqual(failure.exception.status, status)
                self.assertEqual(target.read_bytes(), b'existing target')
                self.assertEqual([path.name for path in Path(tmp).iterdir()], ['target'])
            server.mode = 'healthy'
            metadata = client.download_binary_file('gate:bn:fault', target, revision=1)
            self.assertEqual(target.read_bytes(), b'original bytes')
            self.assertEqual(metadata['bytes'], len(b'original bytes'))
            self.assertEqual(server.paths[-1], '/binary?id=gate%3Abn%3Afault&revision=1')
            # Special characters stay inside the id query, not extra parameters.
            with self.assertRaises(GateError):
                client.download_binary_file('gate:bn:fault&revision=99', target)
            self.assertEqual(server.paths[-1], '/binary?id=gate%3Abn%3Afault%26revision%3D99')

    def test_upload_header_injection_fails_before_any_request(self):
        with fault_server() as (client, server), tempfile.TemporaryDirectory(dir=os.environ['TMPDIR']) as tmp:
            source = Path(tmp) / 'source'
            source.write_bytes(b'bytes')
            for arguments in ({'resource_id': 'bad\r\nX: x'}, {'mime': 'text/plain\r\nX: x'},
                              {'expected_hash': 'bad\r\nX: x'}):
                arguments = {'resource_id': 'gate:bn:fault', **arguments}
                with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                    client.upload_binary(source_path=source, **arguments)
            self.assertEqual(server.paths, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
