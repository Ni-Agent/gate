#!/usr/bin/env python3
"""Isolated tests including real loopback Gate/SSE + native fixture acceptance."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
GO = os.environ.get('GO', '/home/hermes/.local/go/bin/go')

def main():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = ROOT/'evidence'/stamp
    out.mkdir(parents=True)
    (ROOT/'.bin').mkdir(exist_ok=True)
    env = dict(os.environ)
    env['TMPDIR'] = os.environ.get('TMPDIR', str(Path.home()/'.hermes/cache/scratch'))
    env['PATH'] = str(Path(GO).parent) + os.pathsep + env.get('PATH', '')
    commands = [
        ('go-version', [GO, 'version']),
        ('go-race', [GO, 'test', '-race', '-json', '-count=1', '-timeout=240s', './...']),
        ('go-vet', [GO, 'vet', './...']),
        ('go-mod-verify', [GO, 'mod', 'verify']),
        ('build-server', [GO, 'build', '-trimpath', '-o', '.bin/gate-alpha', './cmd/gate-alpha']),
        ('python-client', [sys.executable, '-m', 'unittest', 'client.test_gate_client', 'client.test_followup', '-v']),
        ('binary-version', [str(ROOT/'.bin/gate-alpha'), '--version']),
        ('socket-e2e-default', [sys.executable, 'scripts/e2e_http.py', '--actor', 'hermes:default']),
        ('socket-e2e-prometheus', [sys.executable, 'scripts/e2e_http.py', '--actor', 'hermes:prometheus']),
        ('native-unit-regressions', [sys.executable, '-B', 'scripts/native_units.py', '-v']),
        ('native-server-sse', [sys.executable, '-B', 'scripts/native_acceptance.py']),
        ('native-unknown-operator', [sys.executable, '-B', 'scripts/native_unknown_probe.py']),
        ('runtime-measurement', [sys.executable, 'scripts/measure_native_plugin.py']),
        ('diff-check', ['git', 'diff', '--check']),
    ]
    results = {'created_at': stamp, 'commands': [], 'test_strategy':'Primary acceptance: actual loopback Go server/authenticated async SSE + installed native authority/FIFO/TurnRunner/AIAgents with deterministic HTTPX fixture model. Supplemental Go/handler/security/binary units, real-socket generic client, installed-handler/routing and crash/unknown probes. No configured-provider calls.'}
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True)
    results['source_commit'] = commit.stdout.strip() if commit.returncode == 0 else None
    source_files = [p for p in ROOT.rglob('*') if p.is_file() and '.git' not in p.parts and p.suffix in ('.go', '.py', '.mod', '.sum') and 'evidence' not in p.parts]
    results['source_sha256'] = {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(source_files)}
    for name, command in commands:
        start = time.monotonic()
        completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, timeout=300)
        (out/(name+'.stdout')).write_bytes(completed.stdout)
        (out/(name+'.stderr')).write_bytes(completed.stderr)
        entry = {'name':name, 'command':command, 'returncode':completed.returncode, 'seconds':round(time.monotonic()-start, 3)}
        results['commands'].append(entry)
        print(json.dumps(entry), flush=True)
        if completed.returncode == 0 and name in ('native-server-sse', 'native-unknown-operator'):
            match = re.search(r'"evidence(?:_dir)?":\s*"([^\"]+)"', completed.stdout.decode())
            if not match:
                raise RuntimeError('native acceptance did not identify its evidence directory')
            source = Path(match[1]).resolve()
            if not source.is_relative_to(Path(env['TMPDIR']).resolve()):
                raise RuntimeError('native acceptance evidence escaped scratch')
            shutil.copytree(source, out / name)
        if completed.returncode:
            (out/'results.json').write_text(json.dumps(results, indent=2)+'\n')
            raise SystemExit(f'Failed: {name}; see {out}')
    events = [json.loads(line) for line in (out/'go-race.stdout').read_text().splitlines() if line.strip()]
    passed = [(e.get('Package'),e.get('Test')) for e in events if e.get('Action')=='pass' and e.get('Test')]
    skipped = [e for e in events if e.get('Action')=='skip' and e.get('Test')]
    results['go_tests'] = {'top_level_passed':sum('/' not in test for _,test in passed), 'subtests_passed':sum('/' in test for _,test in passed), 'skipped':len(skipped), 'failed':sum(e.get('Action')=='fail' and bool(e.get('Test')) for e in events)}
    python_log = (out/'python-client.stderr').read_text()
    match = re.search(r'Ran (\d+) tests?', python_log)
    results['python_tests'] = {'passed':int(match[1]) if match else None, 'skipped':0 if 'skipped=' not in python_log else 'unexpected skips'}
    if skipped or 'skipped=' in python_log or not match:
        raise SystemExit('Unexpected skipped or missing verification tests')
    results['artifacts'] = {str(path.relative_to(ROOT)):{'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()} for path in [ROOT/'.bin/gate-alpha']}
    (out/'results.json').write_text(json.dumps(results, indent=2)+'\n')
    (ROOT/'evidence/latest.json').write_text(json.dumps({'directory':str(out),**results},indent=2)+'\n')
    print(json.dumps({'evidence':str(out), 'go_tests':results['go_tests'],'python_tests':results['python_tests']},indent=2))

if __name__=='__main__':
    main()
