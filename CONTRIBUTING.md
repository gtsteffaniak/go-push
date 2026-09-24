# Contributing to go-push

go-push is an **in-process generic pacer** (throttle, debounce, rate-limit, queue). It is not a mobile or web push notification library.

## Repository layout

| Path | Purpose |
|------|---------|
| `push/` | Public library (`package push`) |
| `examples/` | Runnable demos |
| `.github/workflows/` | CI |

## Backwards compatibility

- Exported `package push` types and methods are the semver contract.
- Prefer additive changes: new fields with zero-value = old behavior, new methods.
- Breaking changes require a major release (`/v2` module path).

## Development

```bash
make setup    # optional: install pre-commit hooks
make test
make test-race
make lint
make gofmt
make format
```

## Pull requests

- Run `make test-race`, `make lint`, and `make gofmt` before opening a PR.
- Add tests for behavior changes, especially concurrency and overflow paths.
- Update README and CHANGELOG for user-visible changes.

## Releases

Tag `vX.Y.Z` after LICENSE and CHANGELOG are updated.
