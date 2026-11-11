# Prepare a release

Create a new empty repository at the hosting provider, then add its URL as `origin`. Push the existing history and version tag with `git push -u origin main` and `git push origin v0.1.0-beta.1`. Attach the two Linux archives, source archive, Git bundle, and `SHA256SUMS` from `dist/` to a prerelease named `v0.1.0-beta.1`. Verify the checksums from inside `dist/` with `sha256sum -c SHA256SUMS` before upload. Publish only after reviewing the artifacts and repository visibility.

To rebuild Linux archives with Go 1.26.10 and GNU tar, run `./scripts/build-release.sh`. Build the source archive and bundle from the clean, tagged commit with `./scripts/package-source.sh`. These scripts replace the files in `dist/`.
