# Changelog

All notable changes to this project are documented in this file.

## v1.0.0

### Added

- `New` returns `(*Pacer[T], error)` with config validation (`ErrInvalidConfig`).
- `Push` and `PushContext` return errors: `ErrStopped`, `ErrOverflow`, `ErrTimeout`.
- Overflow policies: `OverflowDropNewest` (default), `OverflowDropOldest`, `OverflowBlock`.
- Config fields: `Overflow`, `PushTimeout`, `DrainOnStop`, `Logger`.
- Injectable `Logger` interface with `NopLogger`, `FromSlog`, and `WithGroup`.
- `Stats()` with `Emitted`, `Dropped`, and `Pending` counters.
- Unpaced queue mode (`ModeQueue` with `Interval: 0`).
- Rate-limit immediate flush of up to `MaxItems` per window.
- `doc.go`, examples, LICENSE, CONTRIBUTING, Makefile, CI, and pre-commit hooks.

### Fixed

- `Updates()` after `Stop()` returns the same closed channel (never `nil`).
- Debounce timer runs in the worker goroutine (no `AfterFunc` send-after-close panic).
- Rate-limit send loop no longer mis-handles blocked consumers.
- Bounded pending memory via `QueueSize` for queue and rate-limit modes.

### Changed

- **Breaking:** `New` signature changed from `*Pacer[T]` to `(*Pacer[T], error)`.
- **Breaking:** `Push` returns `error` instead of silently dropping items.
- **Breaking:** default overflow reports drops via `ErrOverflow` after `PushTimeout` (50ms default).
- **Breaking:** demo moved from module root to `examples/stock/`.
- Output channel is unbuffered except rate-limit mode (buffer = `MaxItems`).

### Migration

See [README.md](README.md#migrating-from-v01).
