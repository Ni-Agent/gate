#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
version=v0.1.0-beta.1
[ "$(git rev-parse HEAD)" = "$(git rev-parse "$version^{commit}")" ] || { echo 'HEAD must match the release tag' >&2; exit 1; }
[ -z "$(git status --porcelain --untracked-files=normal)" ] || { echo 'Working tree must be clean' >&2; exit 1; }
mkdir -p dist
git archive --format=tar --prefix="gate-$version/" "$version" | gzip -n > "dist/gate-$version-source.tar.gz"
git bundle create "dist/gate-$version.bundle" --all
(cd dist && sha256sum *.tar.gz *.bundle > SHA256SUMS)
