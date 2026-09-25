# Nil-Safety and Worker Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all nilaway findings (unguarded `n.transports[...]` lookups), harden worker/concurrency internals, add real task→transport routing, and align the redis submodule with the release guard.

**Architecture:** Add small lookup helpers that return errors for unknown transports; make `jobQueue`/`wg` per-session locals instead of shared struct fields; change `StartWorker` to take a caller-owned `context.Context` (breaking API change); implement `BindTransport` to activate the existing dead `transportHandlers` map; resolve the `"default"` transport sensibly in `NewNakusp`; send missing-handler jobs to the DLQ; make redis/sqlite sends ctx-aware; drop the committed `replace` from `transports/redis/go.mod`.

**Tech Stack:** Go 1.27 multi-module workspace, `github.com/ieshan/idx`, existing `FakeTransport` for tests, `gocheck` Docker quality gates (run from repo root per updated AGENTS.md).

**Verification commands:**
- Tests: `./dev.sh test` (root module covers these changes; redis/sqlite untouched except sends in Task 4)
- Quality gates: the `gocheck` loop in AGENTS.md "Code Quality Checks" — run from repo root
- nilaway must exit 0 on all three modules after Task 3

---

### Task 1: Guarded transport lookup + `ConsumeAll` rewrite

**Files:**
- Modify: `nakusp.go` (add `transport()` helper; rewrite `ConsumeAll`)
- Test: `nakusp_test.go`

`ConsumeAll` currently does `n.transports[transportName]` unguarded (nilaway-flagged pattern) **and** replaces the shared `n.wg` under a read-lock while using the shared `n.jobQueue` channel. Rewrite it to use a guarded lookup, a local `WaitGroup`, and a per-session channel (the `Transport.ConsumeAll` contract already has the transport close the channel — confirmed in all three transports).

- [ ] **Step 1: Write the failing tests** (append as new subtests inside `TestNakusp` in `nakusp_test.go`)

```go
t.Run("ConsumeAllUnknownTransport", func(t *testing.T) {
	var nt nakuspTest
	nt.setup(t)

	err := nt.n.ConsumeAll("nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown transport, got nil")
	}
	if !strings.Contains(err.Error(), `transport "nonexistent" not found`) {
		t.Fatalf("unexpected error: %v", err)
	}
})

t.Run("ConsumeAllConcurrentTransports", func(t *testing.T) {
	f1 := transports.NewFake()
	f2 := transports.NewFake()
	n := NewNakusp(nil, map[string]models.Transport{
		DefaultTransport: f1,
		"second":         f2,
	})
	ctx := context.Background()

	processed := make(chan idx.ID, 4)
	handler := models.Handler{
		MaxRetry: 1,
		Func: func(job *models.Job) error {
			processed <- job.ID
			return nil
		},
	}
	if err := n.AddHandler("test-task", handler); err != nil {
		t.Fatalf("AddHandler returned error: %v", err)
	}

	want := map[idx.ID]bool{}
	for i := 0; i < 2; i++ {
		j1 := &models.Job{ID: idx.NewID(), Name: "test-task"}
		j2 := &models.Job{ID: idx.NewID(), Name: "test-task"}
		want[j1.ID] = true
		want[j2.ID] = true
		if err := f1.Publish(ctx, j1); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}
		if err := f2.Publish(ctx, j2); err != nil {
			t.Fatalf("Publish returned error: %v", err)
		}
	}

	errs := make(chan error, 2)
	go func() { errs <- n.ConsumeAll(DefaultTransport) }()
	go func() { errs <- n.ConsumeAll("second") }()

	for i := 0; i < len(want); i++ {
		select {
		case id := <-processed:
			delete(want, id)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for jobs")
		}
	}
	if err := <-errs; err != nil {
		t.Fatalf("ConsumeAll returned error: %v", err)
	}
	if err := <-errs; err != nil {
		t.Fatalf("ConsumeAll returned error: %v", err)
	}
	if len(want) != 0 {
		t.Fatalf("%d jobs were never processed", len(want))
	}
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -v ./ -run 'TestNakusp/ConsumeAllUnknownTransport|TestNakusp/ConsumeAllConcurrentTransports'`
Expected: FAIL — `ConsumeAllUnknownTransport` panics with nil pointer dereference (not a returned error). `ConsumeAllConcurrentTransports` may pass but is the regression guard for the `n.wg` race (run with `-race`: `go test -race -v ./ -run TestNakusp/ConsumeAllConcurrentTransports` — races against the old `n.wg = &sync.WaitGroup{}` reassignment).

- [ ] **Step 3: Implement**

In `nakusp.go`, add the helper (near `ID()`):

```go
// transport returns the transport registered under name.
func (n *Nakusp) transport(name string) (models.Transport, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	t, ok := n.transports[name]
	if !ok {
		return nil, fmt.Errorf("transport %q not found", name)
	}
	return t, nil
}
```

Rewrite `ConsumeAll` (replaces lines 105–134):

```go
// ConsumeAll consumes all jobs from the specified transport and processes them.
// It starts a consumer goroutine that fetches all jobs and puts them into the job queue.
// It then waits for all jobs to be processed by the workers before returning.
func (n *Nakusp) ConsumeAll(transportName string) error {
	transport, err := n.transport(transportName)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobQueue := make(chan *models.Job, n.config.MaxWorkers)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	go func() {
		errChan <- transport.ConsumeAll(ctx, n.id, jobQueue)
	}()

	for j := range jobQueue {
		wg.Add(1)
		go func(job *models.Job) {
			defer wg.Done()
			if err := n.ExecuteJob(ctx, transport, job); err != nil {
				slog.Error("job execution error", slog.Any("error", err))
			}
		}(j)
	}
	wg.Wait()

	return <-errChan
}
```

Also remove the `jobQueue` field from the `Nakusp` struct (line 33) and its initialization in `NewNakusp` (line 61). To keep this commit compiling, give `StartWorker` a local `jobQueue := make(chan *models.Job, n.config.MaxWorkers)` replacing all `n.jobQueue` uses, **and drop the `close(n.jobQueue)` call** — the field's removal makes the send-on-closed hazard impossible, so it should not survive even one intermediate commit. Keep everything else in `StartWorker` (signature, signal handling) unchanged until Task 3.

- [ ] **Step 4: Run tests**

Run: `go test -race -v ./`
Expected: all pass, including the two new subtests.

- [ ] **Step 5: Commit**

```bash
git add nakusp.go nakusp_test.go
git commit -m "fix: guard transport lookups in ConsumeAll; use per-session queue and WaitGroup"
```

---

### Task 2: `Publish` — routing-aware lookup, no lock over network call

**Files:**
- Modify: `nakusp.go` (add `transportForTask()` helper; rewrite `Publish`)
- Test: `nakusp_test.go`

`Publish` currently holds `n.lock.Lock()` across `transport.Publish` — a blocking network call under a write lock — and dereferences a possibly-nil transport (nilaway finding at line 94).

- [ ] **Step 1: Write the failing test**

```go
t.Run("PublishErrorsWhenDefaultTransportMissing", func(t *testing.T) {
	n := NewNakusp(nil, map[string]models.Transport{
		"a": transports.NewFake(),
		"b": transports.NewFake(),
	})

	_, err := n.Publish(context.Background(), "test-task", "payload")
	if err == nil {
		t.Fatal("expected error when default transport is missing, got nil")
	}
	if !strings.Contains(err.Error(), `transport "default" not found`) {
		t.Fatalf("unexpected error: %v", err)
	}
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./ -run TestNakusp/PublishErrorsWhenDefaultTransportMissing`
Expected: FAIL — panics with nil pointer dereference instead of returning an error.

- [ ] **Step 3: Implement**

Add the helper next to `transport()`:

```go
// transportForTask resolves the transport a task is bound to, falling back to the default.
func (n *Nakusp) transportForTask(taskName string) (models.Transport, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	transportName, ok := n.transportHandlers[taskName]
	if !ok {
		transportName = DefaultTransport
	}
	t, ok := n.transports[transportName]
	if !ok {
		return nil, fmt.Errorf("transport %q not found for task %q", transportName, taskName)
	}
	return t, nil
}
```

Rewrite `Publish` (replaces lines 85–100):

```go
// Publish sends a new job to the appropriate transport based on the task name.
// If no specific transport is registered for the task, it uses the default transport.
func (n *Nakusp) Publish(ctx context.Context, taskName string, payload string) (idx.ID, error) {
	taskId := idx.NewID()
	transport, err := n.transportForTask(taskName)
	if err != nil {
		return taskId, err
	}
	return taskId, transport.Publish(ctx, &models.Job{
		ID:         taskId,
		Name:       taskName,
		Payload:    payload,
		RetryCount: 0,
	})
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race -v ./`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add nakusp.go nakusp_test.go
git commit -m "fix: guard transport lookup in Publish and drop lock over transport call"
```

---

### Task 3: `StartWorker(ctx)` — caller-owned context, per-session queue

**Files:**
- Modify: `nakusp.go` (`StartWorker`, `Nakusp` struct, imports)
- Test: `nakusp_test.go` (update 4 call sites, add 2 tests)

**Breaking change:** `StartWorker(transportName string)` → `StartWorker(ctx context.Context, transportName string)`. Removes the `signal.Notify` handling inside the library — callers own cancellation (e.g. `signal.NotifyContext`). This fixes the nilaway finding at line 153 and removes the `close(n.jobQueue)` send-on-closed hazard.

- [ ] **Step 1: Update existing call sites and write failing tests**

In `nakusp_test.go`, change all four `nt.n.StartWorker(DefaultTransport)` call sites (lines ~308, ~351, ~421, ~491) to:

```go
_ = nt.n.StartWorker(context.Background(), DefaultTransport)
```

Add new subtests:

```go
t.Run("StartWorkerUnknownTransport", func(t *testing.T) {
	var nt nakuspTest
	nt.setup(t)

	err := nt.n.StartWorker(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown transport, got nil")
	}
	if !strings.Contains(err.Error(), `transport "nonexistent" not found`) {
		t.Fatalf("unexpected error: %v", err)
	}
})

t.Run("StartWorkerStopsOnContextCancel", func(t *testing.T) {
	var nt nakuspTest
	nt.setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nt.n.StartWorker(ctx, DefaultTransport) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StartWorker returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StartWorker did not return after context cancellation")
	}
})
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -v ./ -run TestNakusp/StartWorker`
Expected: does not compile (old signature) / `StartWorkerUnknownTransport` would panic — confirms the change is needed.

- [ ] **Step 3: Implement**

Rewrite `StartWorker` (replaces lines 140–189):

```go
// StartWorker begins the job processing loop for a given transport.
// It listens for jobs and executes them in separate goroutines, and returns
// when ctx is cancelled and in-flight jobs have finished. The caller owns
// cancellation — e.g. via signal.NotifyContext for graceful shutdown.
// If scheduled tasks are registered, it also starts a scheduler goroutine
// that publishes jobs at their configured intervals using a single timer-based approach.
func (n *Nakusp) StartWorker(ctx context.Context, transportName string) error {
	transport, err := n.transport(transportName)
	if err != nil {
		return err
	}

	jobQueue := make(chan *models.Job, n.config.MaxWorkers)

	n.wg.Add(2)
	go n.RunUntilCancelled(ctx, transport.Heartbeat)
	go n.RunUntilCancelled(
		ctx,
		func(ctx context.Context, id idx.ID) error {
			return transport.Consume(ctx, id, jobQueue)
		},
	)

	// Start scheduler if there are scheduled tasks
	n.lock.RLock()
	hasSchedules := len(n.schedules) > 0
	n.lock.RUnlock()

	if hasSchedules {
		n.wg.Add(1)
		go n.RunUntilCancelled(ctx, n.runScheduler)
	}

	for {
		select {
		case job := <-jobQueue:
			n.wg.Add(1)
			go func(job *models.Job) {
				defer n.wg.Done()
				if err := n.ExecuteJob(ctx, transport, job); err != nil {
					slog.Error("job execution error", slog.Any("error", err))
				}
			}(job)
		case <-ctx.Done():
			n.wg.Wait()
			return nil
		}
	}
}
```

In the `Nakusp` struct, remove `jobQueue chan *models.Job` (if not already removed in Task 1) and remove the `os`, `os/signal`, `syscall` imports (lines 8–9, 11) — `signal.Notify`/`sigChan` are gone.

Known limitation (unchanged): `n.wg` is still shared across concurrent `StartWorker` calls — out of scope.

- [ ] **Step 4: Run tests**

Run: `go test -race -v ./`
Expected: all pass.

- [ ] **Step 5: Verify nilaway is now clean on root**

Run (from repo root): `gocheck sh -c 'cd /work && nilaway -include-pkgs="github.com/ieshan/nakusp" ./...'`
Expected: exit 0, no output — both nilaway findings resolved.

- [ ] **Step 6: Commit**

```bash
git add nakusp.go nakusp_test.go
git commit -m "feat!: StartWorker takes caller-owned ctx; guard transport lookup

BREAKING CHANGE: StartWorker signature is now StartWorker(ctx, transportName).
Signal handling moved to callers (use signal.NotifyContext)."
```

---

### Task 4: ctx-aware sends in redis and sqlite transports

**Files:**
- Modify: `transports/redis/transport_redis.go:200` (`fetchAndProcessTasks`)
- Modify: `transports/sqlite/transport_sqlite.go:181` (`fetchOnce`)

Both transports do a bare `jobQueue <- job` inside their fetch loops. Now that `StartWorker` never closes the channel, a send with no receiver and a cancelled ctx would block forever (deadlocking `wg.Wait()`). Make both sends ctx-aware.

- [ ] **Step 1: Implement redis fix** — in `fetchAndProcessTasks`:

```go
	count := 0
	for _, task := range tasks {
		job, err = parseJobPayload(task)
		if err != nil {
			// Skip malformed jobs
			continue
		}
		select {
		case jobQueue <- job:
			count++
		case <-ctx.Done():
			return count, ctx.Err()
		}
	}

	return count, nil
```

- [ ] **Step 2: Implement sqlite fix** — in `fetchOnce`, replace `jobQueue <- &job` with:

```go
	select {
	case jobQueue <- &job:
		fetched = true
	case <-ctx.Done():
		return false, ctx.Err()
	}
```

Note: on the ctx path the deferred rollback undoes the UPDATE, so the job correctly returns to `queued`.

- [ ] **Step 3: Run transport tests**

Run: `./dev.sh test` and `./dev.sh test-docker` (redis integration needs the container). At minimum: `(cd transports/sqlite && go test -v ./...)`.
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add transports/redis/transport_redis.go transports/sqlite/transport_sqlite.go
git commit -m "fix: honor ctx cancellation when sending fetched jobs to worker queue"
```

---

### Task 5: `NewNakusp` default-transport resolution + `Close` dedupe

**Files:**
- Modify: `nakusp.go` (`NewNakusp`, `Close`)
- Test: `nakusp_test.go`

Rules: nil or empty map → `FakeTransport` under `"default"`. Single transport under any name → also alias it to `"default"` (keeps original key too). Multiple transports without `"default"` → `slog.Warn`; unrouted `Publish` returns the guarded error from Task 2. Copy the caller's map so `NewNakusp` never mutates caller state. `Close` dedupes so an aliased transport isn't closed twice.

- [ ] **Step 1: Write failing tests**

```go
t.Run("SingleTransportAliasedAsDefault", func(t *testing.T) {
	f := transports.NewFake()
	n := NewNakusp(nil, map[string]models.Transport{"solo": f})

	if _, err := n.Publish(context.Background(), "test-task", "p"); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if len(f.Jobs) != 1 {
		t.Fatalf("expected 1 job in transport, got %d", len(f.Jobs))
	}
	// Close must not double-close the aliased transport
	if err := n.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
})

t.Run("EmptyTransportMapGetsFakeDefault", func(t *testing.T) {
	n := NewNakusp(nil, map[string]models.Transport{})
	if _, err := n.Publish(context.Background(), "test-task", "p"); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
})

t.Run("NewNakuspDoesNotMutateCallerMap", func(t *testing.T) {
	m := map[string]models.Transport{"solo": transports.NewFake()}
	NewNakusp(nil, m)
	if _, ok := m[DefaultTransport]; ok {
		t.Fatal("NewNakusp mutated the caller's transports map")
	}
})
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -v ./ -run 'TestNakusp/SingleTransportAliasedAsDefault|TestNakusp/EmptyTransportMapGetsFakeDefault|TestNakusp/NewNakuspDoesNotMutateCallerMap'`
Expected: `SingleTransportAliasedAsDefault` fails (error from guarded Publish), `EmptyTransportMapGetsFakeDefault` fails, `NewNakuspDoesNotMutateCallerMap` fails after aliasing exists — order matters: these tests are written against the final behavior.

- [ ] **Step 3: Implement**

Rewrite the transport resolution block in `NewNakusp` (replaces lines 48–52):

```go
	ts := make(map[string]models.Transport, len(transports)+1)
	for name, t := range transports {
		ts[name] = t
	}
	if len(ts) == 0 {
		ts[DefaultTransport] = trnspt.NewFake()
	} else if _, ok := ts[DefaultTransport]; !ok {
		if len(ts) == 1 {
			for _, t := range ts {
				ts[DefaultTransport] = t
			}
		} else {
			slog.Warn("no default transport configured; unrouted tasks will fail to publish",
				slog.String("default", DefaultTransport))
		}
	}
```

Then `transports: ts` in the struct literal (was `transports`).

Rewrite `Close` to dedupe aliased transports:

```go
// Close closes all transports and releases any resources.
func (n *Nakusp) Close(ctx context.Context) error {
	seen := make(map[models.Transport]bool, len(n.transports))
	for _, transport := range n.transports {
		if seen[transport] {
			continue
		}
		seen[transport] = true
		if err := transport.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race -v ./`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add nakusp.go nakusp_test.go
git commit -m "fix: resolve a sane default transport in NewNakusp; dedupe Close"
```

---

### Task 6: `BindTransport` — activate task→transport routing

**Files:**
- Modify: `nakusp.go` (add `BindTransport`)
- Test: `nakusp_test.go`

`transportHandlers` was dead code; `transportForTask` (Task 2) already reads it. Add the writer.

- [ ] **Step 1: Write failing tests**

```go
t.Run("BindTransportRoutesTask", func(t *testing.T) {
	f1 := transports.NewFake()
	f2 := transports.NewFake()
	n := NewNakusp(nil, map[string]models.Transport{
		DefaultTransport: f1,
		"redis":          f2,
	})

	if err := n.BindTransport("special-task", "redis"); err != nil {
		t.Fatalf("BindTransport returned error: %v", err)
	}
	if _, err := n.Publish(context.Background(), "special-task", "p1"); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if _, err := n.Publish(context.Background(), "other-task", "p2"); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if len(f2.Jobs) != 1 || f2.Jobs[0].Name != "special-task" {
		t.Fatalf("expected special-task on bound transport, got %v", f2.Jobs)
	}
	if len(f1.Jobs) != 1 || f1.Jobs[0].Name != "other-task" {
		t.Fatalf("expected other-task on default transport, got %v", f1.Jobs)
	}
})

t.Run("BindTransportUnknownTransport", func(t *testing.T) {
	var nt nakuspTest
	nt.setup(t)

	err := nt.n.BindTransport("task", "nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown transport, got nil")
	}
	if !strings.Contains(err.Error(), `transport "nonexistent" not found`) {
		t.Fatalf("unexpected error: %v", err)
	}
})
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -v ./ -run TestNakusp/BindTransport`
Expected: does not compile — `BindTransport` undefined.

- [ ] **Step 3: Implement** (next to `AddHandler`)

```go
// BindTransport routes a task to a named transport instead of the default.
func (n *Nakusp) BindTransport(taskName, transportName string) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	if _, ok := n.transports[transportName]; !ok {
		return fmt.Errorf("transport %q not found", transportName)
	}
	n.transportHandlers[taskName] = transportName
	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race -v ./`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add nakusp.go nakusp_test.go
git commit -m "feat: add BindTransport for per-task transport routing"
```

---

### Task 7: Missing handler → DLQ instead of silent drop

**Files:**
- Modify: `nakusp.go` (`ExecuteJob`)
- Test: `nakusp_test.go`

- [ ] **Step 1: Write failing test**

```go
t.Run("ExecuteJobMissingHandlerSendsToDLQ", func(t *testing.T) {
	var nt nakuspTest
	nt.setup(t)
	ctx := context.Background()

	job := &models.Job{ID: idx.NewID(), Name: "no-handler", Payload: "p"}
	if err := nt.fakeTransport.Publish(ctx, job); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	err := nt.n.ExecuteJob(ctx, nt.fakeTransport, job)
	if err == nil {
		t.Fatal("expected error for missing handler, got nil")
	}
	if len(nt.fakeTransport.Dlq) != 1 || nt.fakeTransport.Dlq[0].ID != job.ID {
		t.Fatalf("expected job in DLQ, got %v", nt.fakeTransport.Dlq)
	}
})
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -v ./ -run TestNakusp/ExecuteJobMissingHandlerSendsToDLQ`
Expected: FAIL — `Dlq` is empty.

- [ ] **Step 3: Implement** — replace the `!ok` branch in `ExecuteJob` (lines 218–222):

```go
	var err error
	if !ok {
		err = fmt.Errorf("handler '%s' not found", job.Name)
		slog.Error("error in ExecuteJob", slog.Any("error", err))
		if dlqErr := transport.SendToDLQ(ctx, job); dlqErr != nil {
			return errors.Join(err, dlqErr)
		}
		return err
	}
```

- [ ] **Step 4: Run tests** — note `ScheduleWithoutHandlerLogsError` must still pass (the `slog.Error` call is preserved).

Run: `go test -race -v ./`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add nakusp.go nakusp_test.go
git commit -m "fix: send jobs with no registered handler to the DLQ"
```

---

### Task 8: Drop committed `replace` from redis go.mod

**Files:**
- Modify: `transports/redis/go.mod`
- Modify: `transports/redis/go.sum` (via tidy)
- Modify: `AGENTS.md` (submodule onboarding step)

The committed `replace github.com/ieshan/nakusp => ../../` makes `./dev.sh release` permanently fail its no-replace guard. sqlite's pattern — `require` the last released version, let `go.work` resolve local code in dev — is the convention to follow.

- [ ] **Step 1: Edit `transports/redis/go.mod`** — remove the `replace` line and change `github.com/ieshan/nakusp v0.0.0` to `v1.0.13`:

```go
module github.com/ieshan/nakusp/transports/redis

go 1.27

require (
	github.com/ieshan/idx v1.3.3
	github.com/ieshan/nakusp v1.0.13
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
```

- [ ] **Step 2: Tidy**

Run: `./dev.sh tidy`
Expected: go.sum entries updated; workspace still resolves nakusp locally — verify with `cd transports/redis && go list -m -f '{{.Dir}}' github.com/ieshan/nakusp`, which should print the repo root path (local resolution via go.work), not a module-cache path.

- [ ] **Step 3: Update AGENTS.md** — in "Practical Agent Workflow" step 4, change the first bullet to match the sqlite pattern:

```markdown
   - Create `transports/<name>/go.mod` requiring the latest released `github.com/ieshan/nakusp` version (no `replace` — `go.work` resolves local code during development)
```

Also update the "Gotchas" bullet about `NewNakusp` to reflect Task 5 behavior:

```markdown
- `NewNakusp(nil, nil)` defaults to FakeTransport. A single custom transport is aliased to "default"; multiple transports require a "default" key or per-task `BindTransport` routing.
```

- [ ] **Step 4: Verify release guard unblocked**

Run: `grep -l '^replace ' go.mod transports/*/go.mod; echo "exit=$?"`
Expected: no matches, `exit=1`.

- [ ] **Step 5: Commit**

```bash
git add transports/redis/go.mod transports/redis/go.sum AGENTS.md go.work.sum
git commit -m "chore: drop committed replace from redis go.mod to unblock releases"
```

---

### Task 9: README + final verification

**Files:**
- Modify: `README.md`
- Verify: whole repo

- [ ] **Step 1: Update README** — both `StartWorker` examples become ctx-based; the "Graceful Shutdown: Signal-based shutdown" feature bullet becomes context-based (caller wires `signal.NotifyContext`); the scheduled-tasks example's manual `sigChan` block becomes:

```go
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := n.StartWorker(ctx, nakusp.DefaultTransport); err != nil {
		fmt.Printf("Worker error: %v\n", err)
	}
```

And the first usage example: `go n.StartWorker(nakusp.DefaultTransport)` → `go n.StartWorker(context.Background(), nakusp.DefaultTransport)`. Optionally document `BindTransport` under the Transports section.

- [ ] **Step 2: Full verification**

Run, in order:
```bash
./dev.sh ci                    # vet + build + test, all modules
./dev.sh test-docker           # redis integration tests
```

Then the gocheck gates from repo root (per updated AGENTS.md):
```bash
for mod in . ./transports/redis ./transports/sqlite; do
    for tool in "go vet" "shadow" "nilness" "golangci-lint run" "gosec" "govulncheck"; do
        gocheck sh -c "cd /work/$mod && $tool ./..." || echo "FAILED: $tool ($mod)"
    done
done
gocheck sh -c 'cd /work && nilaway -include-pkgs="github.com/ieshan/nakusp" ./...'
gocheck sh -c 'cd /work/transports/redis && nilaway -include-pkgs="github.com/ieshan/nakusp/transports/redis" ./...'
gocheck sh -c 'cd /work/transports/sqlite && nilaway -include-pkgs="github.com/ieshan/nakusp/transports/sqlite" ./...'
```
Expected: zero failures, **nilaway clean on all three modules** (previously exit 3 on root).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: update README for ctx-based StartWorker and BindTransport"
```
