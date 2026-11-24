"""Generic Gate transport; the server owns identity, IDs, CAS and commits."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import os
import tempfile
from ipaddress import ip_address
from urllib.parse import quote, urlencode, urlsplit

import httpx

@dataclass
class GateError(Exception):
    status: int
    payload: dict = field(repr=False)

    def __str__(self):
        # Server bodies and transport exceptions must not enter model-facing logs.
        if self.status == 0:
            return 'Gate transport error; outcome unknown; inspect server state before retrying mutations'
        return f'Gate status {self.status}; see structured payload'


def _header(value: str) -> str:
    if not isinstance(value, str) or any(ord(c) < 32 or ord(c) > 126 for c in value):
        raise ValueError('header values must be printable ASCII')
    return value


def _object(response: httpx.Response) -> dict:
    try:
        payload = response.json()
    except ValueError:
        raise GateError(502, {'error': 'invalid JSON response; outcome unknown'}) from None
    if not isinstance(payload, dict):
        raise GateError(502, {'error': 'object response required; outcome unknown'})
    return payload


@dataclass(frozen=True)
class GateClient:
    base_url: str
    token: str = field(repr=False)
    timeout: float = 30
    allow_loopback_http: bool = False

    def __post_init__(self):
        if any(ord(c) <= 32 or ord(c) == 127 for c in self.base_url) or any(c in self.base_url for c in '?#\\'):
            raise ValueError('Gate service origin must not contain whitespace, query, fragment or backslash')
        parsed = urlsplit(self.base_url)
        loopback = False
        try:
            loopback = ip_address(parsed.hostname or '').is_loopback
        except ValueError:
            pass
        if (parsed.scheme != 'https' and not (self.allow_loopback_http and parsed.scheme == 'http' and loopback)
                or not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.path not in ('', '/')):
            raise ValueError('HTTPS service origin required; numeric loopback HTTP requires explicit opt-in')
        parsed.port  # Reject invalid ports before httpx can normalize the origin.
        if not _header(self.token) or any(c.isspace() for c in self.token):
            raise ValueError('nonempty bearer token without whitespace required')
        object.__setattr__(self, 'base_url', self.base_url.rstrip('/'))

    @contextmanager
    def _request(self, method, path, *, headers=None, **arguments):
        try:
            with httpx.Client(timeout=self.timeout, trust_env=False, follow_redirects=False) as client:
                with client.stream(method, self.base_url + path,
                                   headers={'Authorization': 'Bearer ' + self.token, **(headers or {})}, **arguments) as response:
                    if response.status_code != 200:
                        try:
                            response.read()
                            payload = _object(response)
                        except (GateError, httpx.HTTPError):
                            payload = {'error': 'unreadable server error response'}
                        raise GateError(response.status_code, payload)
                    yield response
        except httpx.HTTPError:
            raise GateError(0, {'error': 'transport error; outcome unknown; inspect server state before retrying mutations'}) from None

    def authenticate(self, expected_identity=None):
        with self._request('GET', '/whoami') as response:
            response.read()
            who = _object(response)
        identity = who.get('identity')
        if who.get('protocol') != 'gate-alpha-native-1' or not isinstance(identity, str) or not identity:
            raise GateError(403, {'error': 'unexpected Gate service/account'})
        if expected_identity is not None and identity != expected_identity:
            raise GateError(403, {'error': 'Gate account does not match configured identity'})
        return identity

    def rpc(self, operation, **arguments):
        body = json.dumps(arguments, ensure_ascii=False, allow_nan=False).encode('utf-8')
        with self._request('POST', '/rpc/' + quote(operation, safe=''), content=body,
                           headers={'Content-Type': 'application/json'}) as response:
            response.read()
            return _object(response)

    def upload_binary(self, resource_id, source_path, mime='application/octet-stream', *,
                      expected_revision=None, expected_hash=None, tags=()):
        update = expected_revision is not None or expected_hash is not None
        headers = {'Content-Type': 'application/octet-stream', 'X-Gate-ID': _header(resource_id),
                   'X-Gate-Mime': _header(mime)}
        if expected_revision is not None:
            headers['X-Gate-Revision'] = _header(str(expected_revision))
        if expected_hash is not None:
            headers['If-Match'] = _header(expected_hash)
        if tags:
            headers['X-Gate-Tags'] = json.dumps(list(tags), ensure_ascii=True)
        with open(source_path, 'rb') as source:
            headers['Content-Length'] = str(os.fstat(source.fileno()).st_size)
            with self._request('PUT' if update else 'POST', '/binary', content=source, headers=headers) as response:
                response.read()
                return _object(response)

    def download_binary_file(self, resource_id, path, revision=None):
        """Stream/hash-check in a sibling staging file, then atomically publish."""
        query = {'id': resource_id}
        if revision is not None:
            query['revision'] = revision
        path = os.path.abspath(path)
        temporary = None
        try:
            with self._request('GET', '/binary?' + urlencode(query)) as response:
                metadata = {'id': response.headers.get('X-Gate-ID'), 'revision': response.headers.get('X-Gate-Revision'),
                            'mime': response.headers.get('X-Gate-Mime'), 'token': response.headers.get('ETag', '').strip('"')}
                if (response.headers.get('Content-Type', '').split(';')[0] != 'application/octet-stream'
                        or metadata['id'] != resource_id
                        or revision is not None and metadata['revision'] != str(revision)):
                    raise GateError(502, {'error': 'binary response metadata mismatch'})
                fd, temporary = tempfile.mkstemp(prefix='.gate-download-', dir=os.path.dirname(path))
                digest, size = hashlib.sha256(), 0
                with os.fdopen(fd, 'wb') as target:
                    for chunk in response.iter_raw(65536):
                        target.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                    if (response.headers.get('Content-Length', str(size)) != str(size)
                            or metadata['token'] != digest.hexdigest()):
                        raise GateError(502, {'error': 'binary length or digest mismatch'})
                    target.flush()
                    os.fsync(target.fileno())
                os.replace(temporary, path)
                metadata['bytes'] = size
                return metadata
        finally:
            if temporary is not None and os.path.exists(temporary):
                os.unlink(temporary)
