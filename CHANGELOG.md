# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Fixed

- `Stop` with `DrainOnStop` no longer blocks forever when nothing is reading `Updates`. Each remaining item is offered for at most `PushTimeout`; on timeout the rest are counted in `Stats.Dropped` and `Stop` returns.
- Queue and rate-limit modes no longer release an in-flight reservation before the item is retained. A `Push` that returns nil stays retained unless `OverflowDropOldest` evicts it or a `DrainOnStop` send times out.

### Removed

- Deleted the committed `stock` ELF at the repository root and ignored `/stock`. The demo remains `examples/stock`.

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
