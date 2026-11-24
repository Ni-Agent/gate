#!/usr/bin/env python3
"""Conservative staged-file credential/binary review; not a secret-proof claim."""
import json
from pathlib import Path
import re
import subprocess

ROOT=Path(__file__).resolve().parents[1]
paths=subprocess.check_output(['git','diff','--cached','--name-only','-z'],cwd=ROOT).decode().split('\0')
patterns=[r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',r'\b(?:sk-proj-|sk-ant-|ghp_|github_pat_)[A-Za-z0-9_-]{16,}',r'(?i)Bearer [A-Za-z0-9_+/=-]{32,}']
findings=[];count=0
for name in filter(None,paths):
    p=ROOT/name
    if not p.is_file():continue
    count+=1
    if name.startswith(('.bin/','evidence/')) or p.name=='.env' or p.suffix in ('.db','.sqlite','.pyc'):
        findings.append({'file':name,'reason':'prohibited artifact'});continue
    data=p.read_bytes()
    if b'\0' in data:findings.append({'file':name,'reason':'binary content'});continue
    text=data.decode()
    for pattern in patterns:
        if re.search(pattern,text):findings.append({'file':name,'reason':'credential-like pattern'})
print(json.dumps({'staged_files_scanned':count,'findings':findings,'limitation':'pattern scan plus scoped file review, not a guarantee that arbitrary text contains no secret'},indent=2))
raise SystemExit(bool(findings))
