#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
rm -rf dist
mkdir dist
for arch in amd64 arm64; do
  dir="dist/gate-linux-$arch"
  mkdir "$dir"
  for cmd in gate gate-init gate-admin; do
    CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o "$dir/$cmd" "./cmd/$cmd"
  done
  cp LICENSE README.md SETUP.md "$dir/"
  cp scripts/gate-local "$dir/gate-local"
  cp -R docs deploy examples "$dir/"
  tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner -czf "dist/gate-v0.1.0-beta.1-linux-$arch.tar.gz" -C dist "gate-linux-$arch"
  rm -rf "$dir"
done
(cd dist && sha256sum *.tar.gz > SHA256SUMS)
