#!/usr/bin/env python3
"""Compare the full native connector plus its generic client, without test code."""
import ast
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tokenize

ROOT = Path(__file__).resolve().parents[1]
BASE = '9bf33ef4027eab04e39f2958f21ef0383e36c84f'


def measure(sources):
    files = {}
    for name, source in sorted(sources.items()):
        tree = ast.parse(source)
        doclines = set()
        for node in ast.walk(tree):
            body = getattr(node, 'body', None)
            if (isinstance(body, list) and body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str)):
                doclines.update(range(body[0].lineno, (body[0].end_lineno or body[0].lineno) + 1))
        code = set()
        ignored = {tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.INDENT,
                   tokenize.DEDENT, tokenize.ENDMARKER, tokenize.ENCODING}
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type not in ignored:
                code.update(range(token.start[0], token.end[0] + 1))
        files[name] = {
            'physical_lines': len(source.splitlines()),
            'code_lines_excluding_blank_comments_docstrings': len(code - doclines),
            'statements': sum(isinstance(node, ast.stmt) and not (
                isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)) for node in ast.walk(tree)),
            'functions': sum(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in ast.walk(tree)),
            'sha256': hashlib.sha256(source.encode()).hexdigest(),
        }
    metrics = ('physical_lines', 'code_lines_excluding_blank_comments_docstrings', 'statements', 'functions')
    return {'files': files, 'totals': {key: sum(row[key] for row in files.values()) for key in metrics}}


def main():
    tracked = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', BASE], cwd=ROOT, text=True).splitlines()
    old_names = [name for name in tracked if (name.startswith('native_gate/') and name.endswith('.py'))
                 or name in ('client/gate_client.py', 'client/hermes_bridge.py')]
    before = measure({name: subprocess.check_output(['git', 'show', BASE + ':' + name], cwd=ROOT, text=True)
                      for name in old_names})
    current = list((ROOT / 'native_gate').glob('*.py')) + [ROOT / 'client/gate_client.py']
    after = measure({str(path.relative_to(ROOT)): path.read_text() for path in current})
    print(json.dumps({
        'baseline_commit': BASE,
        'scope': 'All native_gate Python runtime files plus the complete generic client and former bridge; tests, docs, manifests, server and Hermes core excluded equally.',
        'before': before,
        'after': after,
        'reduction_percent': {key: round(100 * (1 - after['totals'][key] / value), 1)
                              for key, value in before['totals'].items()},
        'caveat': 'Line counts depend on formatting; AST statements are a structural size indicator, not a formal complexity score.',
    }, indent=2))


if __name__ == '__main__':
    main()
