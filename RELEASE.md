# Releasing the Go SDK

The module lives at the repository root and uses `vX.Y.Z` tags. The tag determines the module version; there is no separate package-version field to update in `go.mod`. Legacy `go/vX.Y.Z` tags belong to the former monorepo and are retained as history.

## Prepare the release PR

Compare changes with the latest published tag and choose the next version. For a pre-1.0 release, API-breaking changes require a new minor version. Move the Unreleased entries into a dated `## vX.Y.Z (YYYY-MM-DD)` section in [CHANGELOG.md](CHANGELOG.md), leaving an empty Unreleased section for subsequent work.

Use `*` bullets and the nonempty sections Breaking Changes, New Features, Bug Fixes, and Improvements, in that order. Describe user-observable changes and link breaking changes to the corresponding migration guide. Preserve published entries. Keep implementation refactors, test maintenance, and transient release status out of user-facing notes.

Verify the candidate using the [local checks](CONTRIBUTING.md#local-checks). Review exported API documentation, compile examples, and check repository links and the final diff. Update `go.mod` and `go.sum` only when the module's requirements actually change. Open a release PR with its compatibility changes and validation results.

## Publish the approved commit

After the release PR is merged and publication is authorized, select its commit and version. Replace the placeholders below with the release heading and approved commit:

```sh
release_version=vX.Y.Z
release_commit=COMMIT_SHA
git tag -a "$release_version" "$release_commit" -m "Release $release_version for Go SDK"
git push origin "$release_version"
```

Create the GitHub release for that tag using the matching changelog section as its notes. Never move a published version tag; corrections require another version.

## Verify availability

After the module proxy has observed the tag, use a fresh temporary module with `release_version` still set:

```sh
release_tmp=$(mktemp -d)
cd "$release_tmp"
go mod init example.com/scopedb-release-check
go get "github.com/scopedb/goscopedb@$release_version"
go list -m github.com/scopedb/goscopedb
go doc github.com/scopedb/goscopedb
```

Confirm that the resolved version matches the tag and the package documentation is available.
