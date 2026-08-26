# Releasing the ScopeDB Go SDK

The Go module lives at the repository root, so release tags use the `vX.Y.Z` form. The examples below use `v0.6.3`. Tags with the legacy `go/vX.Y.Z` form were retained only to preserve release history from the former monorepo.

## Prepare and verify

From the repository root:

```sh
go fmt ./...
go mod tidy
git diff --exit-code -- go.mod go.sum
go test ./...
go test -race ./...
go vet ./...
go doc .
```

Review the public surface and release material:

```sh
go list -f '{{range .GoFiles}}{{$.Dir}}/{{.}}{{"\n"}}{{end}}' .
git diff --check
git diff -- README.md doc.go CHANGELOG.md RELEASE.md examples
rg -n 'https?://|SCOPEDB_' README.md doc.go CHANGELOG.md RELEASE.md examples
```

Confirm that `CHANGELOG.md` contains the intended release date, the examples compile, no secret or private endpoint is present, and the final diff contains only intentional changes. Commit and merge the release preparation before tagging.

## Tag after acknowledgement

Immediately before publishing, obtain the required explicit acknowledgement. Then, from the repository root, create the annotated module tag and push only that tag:

```sh
version=v0.6.3
git tag -a "$version" -m "Release $version for Go SDK"
git push origin "$version"
```

This runbook documents the commands; preparing a release does not authorize running the tag or push commands.

## Verify the published module

After the module proxy has observed the tag, verify it from a fresh temporary module:

```sh
release_tmp=$(mktemp -d)
cd "$release_tmp"
go mod init example.com/scopedb-release-check
go get github.com/scopedb/goscopedb@v0.6.3
go list -m github.com/scopedb/goscopedb
go doc github.com/scopedb/goscopedb
```

References:

- [Mapping versions to commits](https://go.dev/ref/mod#vcs-version)
- [Module version numbering](https://go.dev/doc/modules/version-numbers)
