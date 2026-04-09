# Phase 2 Observability, HTTP Ergonomics, and Integration Usability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add observability primitives first, then improve HTTP middleware extensibility, then add integration helpers without breaking the existing public API.

**Architecture:** Introduce a small observer contract in the core package so middleware and future integration points can emit structured events consistently. Extend middleware through additive options and a new constructor, keeping `Middleware(...)` as a compatibility wrapper. Build usability helpers on top of those extension points rather than duplicating behavior.

**Tech Stack:** Go, `net/http`, `log/slog`, atomic counters, existing testify-based tests.

---

### Task 1: Add Observability Primitives

**Files:**
- Create: `observability.go`
- Modify: `middleware/http.go`
- Modify: `middleware/http_test.go`
- Modify: `README.md`

- [ ] **Step 1: Write the failing tests**

Add tests covering:
- middleware emits an `allowed` event with key, algorithm, status, and duration
- middleware emits a `denied` event with retry metadata
- middleware emits an `error` event when limiter check fails
- built-in counters increment correctly across those outcomes

- [ ] **Step 2: Run the focused middleware tests to verify they fail**

Run: `go test ./middleware -run 'TestMiddleware_Observability|TestMiddleware_Counters'`
Expected: FAIL because observer types and option-aware middleware do not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Add a root-level observer API in `observability.go` with:
- `type EventKind string` for `allowed`, `denied`, `error`
- `type Event struct` containing key, algorithm, kind, result fields, error, and duration
- `type Observer interface` plus `ObserverFunc`
- `type Counters struct` implementing `Observe(context.Context, Event)` with atomic counts
- `type SlogObserver struct` or constructor for structured logging

Update middleware to:
- keep `Middleware(...)` working unchanged for existing callers
- add `MiddlewareWithOptions(...)` plus `WithObserver(...)`
- emit a single event per request after limiter check completes

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `go test ./middleware -run 'TestMiddleware_Observability|TestMiddleware_Counters'`
Expected: PASS.

- [ ] **Step 5: Run broader verification for the task**

Run: `go test ./...`
Expected: PASS.

### Task 2: Add HTTP Middleware Ergonomics

**Files:**
- Modify: `middleware/http.go`
- Modify: `middleware/http_test.go`
- Modify: `README.md`

- [ ] **Step 1: Write the failing tests**

Add tests covering:
- custom denied handler overrides the default JSON response
- custom error handler can fail-open or return a caller-defined response
- standard rate-limit headers are set through an option without breaking existing header behavior

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `go test ./middleware -run 'TestMiddleware_CustomHandlers|TestMiddleware_FailOpen|TestMiddleware_StandardHeaders'`
Expected: FAIL because the new options do not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Add middleware options for:
- custom denied handler
- custom limiter error handler
- fail-open behavior
- optional standard rate-limit header emission

Preserve the existing `Middleware(...)` behavior by keeping defaults identical.

- [ ] **Step 4: Run focused tests to verify they pass**

Run: `go test ./middleware -run 'TestMiddleware_CustomHandlers|TestMiddleware_FailOpen|TestMiddleware_StandardHeaders'`
Expected: PASS.

- [ ] **Step 5: Run broader verification for the task**

Run: `go test ./...`
Expected: PASS.

### Task 3: Add Integration Usability Helpers

**Files:**
- Create: `http_helpers.go`
- Modify: `model.go`
- Modify: `model_test.go`
- Modify: `README.md`

- [ ] **Step 1: Write the failing tests**

Add tests covering:
- helper key extractors for common request sources (header, IP)
- config helper behavior for per-request key application without mutating shared config
- clear defaults/documented helper APIs for common middleware wiring

- [ ] **Step 2: Run focused tests to verify they fail**

Run: `go test ./... -run 'TestHeaderKeyFunc|TestRemoteIPKeyFunc|TestConfigWithKey'`
Expected: FAIL because the helpers do not exist yet.

- [ ] **Step 3: Write the minimal implementation**

Add small helper APIs that compose with the existing middleware, for example:
- header-based key extraction helper
- remote-IP extraction helper that handles host:port correctly
- config cloning helper for applying keys safely

Do not introduce framework-specific integrations in this task.

- [ ] **Step 4: Run focused tests to verify they pass**

Run: `go test ./... -run 'TestHeaderKeyFunc|TestRemoteIPKeyFunc|TestConfigWithKey'`
Expected: PASS.

- [ ] **Step 5: Run final verification**

Run: `go test ./... && go test -race ./... && go vet ./...`
Expected: PASS.