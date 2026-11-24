"""Public transport contract; main acceptance uses real Go/HTTP/disk."""
from dataclasses import is_dataclass
import unittest

from client.gate_client import GateClient, GateError
from scripts.e2e_http import run


class ConfigurationTests(unittest.TestCase):
    def test_dataclass_configuration_and_secret_free_repr(self):
        client = GateClient('https://example.invalid', 'synthetic-private-token')
        self.assertTrue(is_dataclass(client))
        self.assertTrue(is_dataclass(GateError))
        self.assertEqual(client.token, 'synthetic-private-token')
        self.assertNotIn(client.token, repr(client))
        self.assertNotIn('synthetic-private-token', str(GateError(400, {'error': 'synthetic-private-token'})))

    def test_origin_and_header_safety(self):
        for origin in ('http://example.invalid', 'http://localhost', 'https://user@example.invalid',
                       'https://:password@example.invalid', 'https://example.invalid/path',
                       'https://example.invalid?x=1', 'https://example.invalid#fragment',
                       'https://example.invalid?', 'https://example.invalid#',
                       'https://example.invalid\n', 'https://example.invalid:bad'):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                GateClient(origin, 'synthetic', allow_loopback_http=True)
        for token in ('', 'unsafe\r\nX-Injected: yes', 'non-ascii-\u00e9'):
            with self.subTest(token=token), self.assertRaises(ValueError):
                GateClient('https://example.invalid', token)
        with self.assertRaises(ValueError):
            GateClient('http://127.0.0.1', 'synthetic')
        GateClient('http://127.0.0.1:12345', 'synthetic', allow_loopback_http=True)
        GateClient('http://[::1]:12345', 'synthetic', allow_loopback_http=True)


class RealSocketAcceptance(unittest.TestCase):
    def test_generic_client_against_go_server(self):
        result = run()
        self.assertEqual(result['result'], 'PASS')
        self.assertEqual(result['binary']['bytes'], 12 << 20)
        self.assertEqual(result['one_request_mutations'], 12)


if __name__ == '__main__':
    unittest.main(verbosity=2)
