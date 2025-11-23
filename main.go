package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	indexName       = "idx"
	tableName       = "t"
	schemaTableName = "public." + tableName
	seedCount       = 1_000_000
	postgresVersion = "16.8"

	writerTickInterval = 10 * time.Millisecond
	probeTickInterval  = 100 * time.Millisecond
	cicPollInterval    = 1 * time.Millisecond
	cicStateTimeout    = 10 * time.Second
)

var scenarioClock = &scenarioTimer{}

// main executes each scenario so we can observe how CREATE INDEX IF NOT EXISTS behaves across lifecycle states.
func main() {
	log.SetFlags(0)
	log.SetPrefix("")
	log.SetOutput(&relativeWriter{clock: scenarioClock, out: os.Stderr})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	scenarios := []scenario{
		{key: "sequential", name: "sequential: fully ready + valid", run: runSequentialScenario},
		{key: "before-catalog", name: "state: before catalog entry", run: runOverlapBeforeCatalogScenario},
		{key: "exists-not-ready", name: "state: exists but not ready", run: runOverlapBeforeReadyScenario},
		{key: "ready-not-valid", name: "state: ready but not valid", run: runOverlapBeforeValidScenario},
		{key: "validation-phase", name: "state: validation phase before commit", run: runValidationPhaseScenario},
	}

	targets := selectScenarioIndexes(os.Args[1:], scenarios)
	for _, idx := range targets {
		if err := ctx.Err(); err != nil {
			log.Printf("context canceled, stopping remaining scenarios: %v", err)
			return
		}
		sc := scenarios[idx]
		if idx != 0 {
			fmt.Fprintln(os.Stderr)
		}
		scenarioClock.Reset(time.Now())
		log.Printf("==== Scenario %d: %s ====", idx+1, sc.name)
		if err := sc.run(ctx, sc.key); err != nil {
			log.Printf("[%s] finished with error: %v", sc.key, err)
		}
	}
}

// scenario describes a runnable case in the index-state matrix.
type scenario struct {
	key  string
	name string
	run  func(context.Context, string) error
}

// cicEvent conveys progress updates from the CIC monitor to the main goroutine.
type cicEvent struct {
	snapshot indexSnapshot
	err      error
}

// selectScenarioIndexes resolves CLI arguments into scenario indexes, accepting numbers, keys, names, or "all".
func selectScenarioIndexes(args []string, scenarios []scenario) []int {
	if len(args) == 0 {
		return allScenarioIndexes(scenarios)
	}

	keyMap := make(map[string]int, len(scenarios))
	for i, sc := range scenarios {
		keyMap[strings.ToLower(sc.key)] = i
		keyMap[strings.ToLower(sc.name)] = i
	}

	selected := make(map[int]struct{})
	for _, arg := range args {
		arg = strings.ToLower(strings.TrimSpace(arg))
		if arg == "" {
			continue
		}
		if arg == "all" {
			return allScenarioIndexes(scenarios)
		}
		if n, err := strconv.Atoi(arg); err == nil {
			if n >= 1 && n <= len(scenarios) {
				selected[n-1] = struct{}{}
				continue
			}
			log.Printf("ignoring out-of-range scenario number %q (valid 1-%d)", arg, len(scenarios))
			continue
		}
		if idx, ok := keyMap[arg]; ok {
			selected[idx] = struct{}{}
			continue
		}
		log.Printf("ignoring unknown scenario %q; use 1-%d or keywords like %q", arg, len(scenarios), scenarios[0].key)
	}

	if len(selected) == 0 {
		return allScenarioIndexes(scenarios)
	}

	var ordered []int
	for i := range scenarios {
		if _, ok := selected[i]; ok {
			ordered = append(ordered, i)
		}
	}
	return ordered
}

// allScenarioIndexes returns every scenario index, preserving declaration order.
func allScenarioIndexes(scenarios []scenario) []int {
	indexes := make([]int, len(scenarios))
	for i := range scenarios {
		indexes[i] = i
	}
	return indexes
}

// formatMillis normalizes duration logging to milliseconds with two decimal places.
func formatMillis(d time.Duration) string {
	return fmt.Sprintf("%.2f ms", float64(d)/float64(time.Millisecond))
}

// logIFNTrigger ensures every IF NOT EXISTS invocation announces its trigger uniformly.
func logIFNTrigger(prefix, trigger string) {
	log.Printf("%s IF NOT EXISTS trigger: %s", prefix, trigger)
}

// runSequentialScenario demonstrates the simplest path: finish the concurrent build, let Postgres mark the index
// ready+valid, then immediately run CREATE INDEX IF NOT EXISTS in the same connection. This mirrors the “all good”
// deployment where the follow-up check should be a no-op.
func runSequentialScenario(ctx context.Context, label string) error {
	prefix := "[" + label + "]"
	return withPostgres(ctx, label, func(ctx context.Context, db *pgx.Conn, dsn string) error {
		prepareTable(ctx, db, prefix)
		stopWriters := startWriters(ctx, dsn)
		defer stopWriters()

		log.Printf("%s starting CREATE INDEX CONCURRENTLY %s", prefix, indexName)
		start := time.Now()
		mustExec(ctx, db, fmt.Sprintf(`create index concurrently if not exists %s on %s(email)`, indexName, tableName))
		log.Printf("%s CIC finished in %s", prefix, formatMillis(time.Since(start)))

		state, err := fetchIndexState(ctx, db, indexName)
		if err != nil {
			return fmt.Errorf("%s fetch index state prior to IF NOT EXISTS: %w", prefix, err)
		}
		logIFNTrigger(prefix, state.String())
		if err := runNonConcurrentCreate(ctx, db, prefix); err != nil {
			return err
		}

		return nil
	})
}

// type overlapTrigger enumerates the catalog states we want to target before invoking IF NOT EXISTS.
type overlapTrigger int

const (
	// triggerBeforeCatalog fires before any catalog entry exists.
	triggerBeforeCatalog overlapTrigger = iota
	// triggerAfterCatalogBeforeReady waits until a catalog row exists but the index is not yet marked ready.
	triggerAfterCatalogBeforeReady
	// triggerReadyBeforeValid waits until indisready is true yet indisvalid remains false.
	triggerReadyBeforeValid
	// triggerValidationPhase waits until the CIC session is running validation work before committing.
	triggerValidationPhase
)

// runOverlapBeforeCatalogScenario fires IF NOT EXISTS before the CIC session inserts any pg_index row. This is the
// “name reuse with missing catalog entry” case where the non-concurrent command creates the entry first and forces the
// concurrent build to error.
func runOverlapBeforeCatalogScenario(ctx context.Context, label string) error {
	return runOverlapScenario(ctx, label, triggerBeforeCatalog)
}

// runOverlapBeforeReadyScenario waits until pg_index.contains the row but `indisready=false`, meaning the CIC session
// is still scanning. This is the overlap where IF NOT EXISTS collides with an incomplete build and can deadlock with
// ShareUpdateExclusive locks.
func runOverlapBeforeReadyScenario(ctx context.Context, label string) error {
	return runOverlapScenario(ctx, label, triggerAfterCatalogBeforeReady)
}

// runOverlapBeforeValidScenario triggers once `indisready=true` yet `indisvalid=false`, which reflects the window
// between build completion and validation. Writers are fully blocked here but the concurrent build is still running.
func runOverlapBeforeValidScenario(ctx context.Context, label string) error {
	return runOverlapScenario(ctx, label, triggerReadyBeforeValid)
}

// runValidationPhaseScenario approximates the validation overlap prior to commit (catalog still shows valid=false).
func runValidationPhaseScenario(ctx context.Context, label string) error {
	return runOverlapScenario(ctx, label, triggerValidationPhase)
}

// runOverlapScenario is the shared harness for the overlapping scenarios: it starts a CIC in the background, waits
// for the requested catalog state, spawns writer probes, and then runs CREATE INDEX IF NOT EXISTS so we can observe
// locks, timings, and errors for that exact lifecycle stage.
func runOverlapScenario(ctx context.Context, label string, trigger overlapTrigger) error {
	prefix := "[" + label + "]"
	return withPostgres(ctx, label, func(ctx context.Context, db *pgx.Conn, dsn string) error {
		prepareTable(ctx, db, prefix)
		stopWriters := startWriters(ctx, dsn)
		defer stopWriters()
		stopProbe := startProbe(ctx, dsn)
		defer stopProbe()

		cicSQL := fmt.Sprintf(`create index concurrently if not exists %s on %s(email)`, indexName, tableName)
		ncSQL := fmt.Sprintf(`create index if not exists %s on %s(email)`, indexName, tableName)

		log.Printf("%s launching CREATE INDEX CONCURRENTLY in background", prefix)
		events, cicDone := startCICMonitor(ctx, dsn, cicSQL)

		var runErr error
		switch trigger {
		case triggerBeforeCatalog:
			log.Printf("%s issuing non-concurrent call before catalog entry exists (best effort)", prefix)
			state, err := fetchIndexState(ctx, db, indexName)
			if err != nil {
				return fmt.Errorf("%s fetch index state: %w", prefix, err)
			}
			logIFNTrigger(prefix, state.String())
			runErr = runNonConcurrentWithSQL(ctx, db, prefix, ncSQL)
		case triggerAfterCatalogBeforeReady:
			log.Printf("%s waiting for catalog entry but not yet ready", prefix)
			snap, err := waitForCICEvent(events, cicStateTimeout, func(s indexSnapshot) bool {
				return s.state.exists && !s.state.ready && s.progressActive
			})
			if err != nil {
				return fmt.Errorf("%s wait for not-ready catalog entry: %w", prefix, err)
			}
			logIFNTrigger(prefix, snap.state.String())
			runErr = runNonConcurrentWithSQL(ctx, db, prefix, ncSQL)
		case triggerReadyBeforeValid:
			log.Printf("%s waiting for catalog entry that is ready but not yet valid", prefix)
			snap, err := waitForCICEvent(events, cicStateTimeout, func(s indexSnapshot) bool {
				return s.state.exists && s.state.ready && !s.state.valid && s.progressActive
			})
			if err != nil {
				return fmt.Errorf("%s wait for ready-but-invalid index: %w", prefix, err)
			}
			logIFNTrigger(prefix, snap.state.String())
			// -> ERROR: deadlock detected -> cancel CIC (most of the time)
			// once observed canceling non-concurrent CREATE INDEX
			// once passed without ERROR
			runErr = runNonConcurrentWithSQL(ctx, db, prefix, ncSQL)
		case triggerValidationPhase:
			log.Printf("%s waiting for catalog entry that is ready and valid (but CIC not committed)", prefix)
			snap, err := waitForCICEvent(events, cicStateTimeout, func(s indexSnapshot) bool {
				// -> SUCCESS (most of the time)
				return s.progressActive && strings.Contains(s.progressPhase, "index validation")
			})
			if err != nil {
				return fmt.Errorf("%s wait for ready+valid index: %w", prefix, err)
			}
			logIFNTrigger(prefix, snap.state.String())
			runErr = runNonConcurrentWithSQL(ctx, db, prefix, ncSQL)
		default:
			return fmt.Errorf("unknown trigger %d", trigger)
		}

		drainCICEvents(events)

		cicErr := <-cicDone
		if runErr != nil {
			return runErr
		}
		return cicErr
	})
}

// withPostgres provisions a Postgres container, connects, and runs fn with cleanup.
func withPostgres(ctx context.Context, label string, fn func(context.Context, *pgx.Conn, string) error) error {
	log.Printf("[%s] starting Postgres %s container", label, postgresVersion)
	pgC, err := postgres.Run(ctx, "postgres:"+postgresVersion,
		postgres.WithDatabase("db"),
		postgres.WithUsername("u"),
		postgres.WithPassword("pw"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	defer pgC.Terminate(ctx)

	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("connection string: %w", err)
	}
	log.Printf("[%s] Postgres up at %s", label, dsn)

	db := mustConnect(ctx, dsn)
	defer db.Close(ctx)

	return fn(ctx, db, dsn)
}

// mustConnect opens a pgx connection or terminates on failure.
func mustConnect(ctx context.Context, dsn string) *pgx.Conn {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	return conn
}

// prepareTable creates the target table and seeds it with seedCount rows.
func prepareTable(ctx context.Context, db *pgx.Conn, prefix string) {
	log.Printf("%s creating table %s", prefix, tableName)
	mustExec(ctx, db, fmt.Sprintf(`create table %s (id bigserial primary key, email text)`, tableName))

	start := time.Now()
	mustExec(ctx, db, fmt.Sprintf(`
insert into %s (email)
select 'user'||g||'@example.com' from generate_series(1, %d) g`, tableName, seedCount))
	log.Printf("%s seed of %d rows completed in %s", prefix, seedCount, formatMillis(time.Since(start)))
}

// startWriters launches a goroutine that keeps pressure on the table via periodic writes.
func startWriters(ctx context.Context, dsn string) func() {
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		conn := mustConnect(wctx, dsn)
		defer conn.Close(wctx)

		log.Println("[writers] started")
		ticker := time.NewTicker(writerTickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-wctx.Done():
				log.Println("[writers] stopping")
				return
			case <-ticker.C:
				conn.Exec(wctx, fmt.Sprintf(`update %s set email = email where id %% 500000 = 0`, tableName))
				conn.Exec(wctx, fmt.Sprintf(`insert into %s (email) values ('hot@example.com')`, tableName))
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// logLocks prints the lock modes currently observed for the test table.
func logLocks(ctx context.Context, db *pgx.Conn, label string) {
	log.Printf("%s: %s", label, sampleLocks(ctx, db))
}

// mustExec runs the provided SQL, terminating the program on error.
func mustExec(ctx context.Context, db *pgx.Conn, sql string) {
	if _, err := db.Exec(ctx, sql); err != nil {
		log.Fatalf("exec failed: %v\nSQL: %s", err, sql)
	}
}

// execSQL runs SQL and returns any error so callers can choose their own failure handling.
func execSQL(ctx context.Context, db *pgx.Conn, sql string) error {
	_, err := db.Exec(ctx, sql)
	return err
}

// queryOneStr executes a scalar query and returns the result or the error string.
func queryOneStr(ctx context.Context, db *pgx.Conn, sql string) string {
	var s string
	if err := db.QueryRow(ctx, sql).Scan(&s); err != nil {
		return fmt.Sprintf("err: %v", err)
	}
	return s
}

// sampleLocks aggregates the lock modes that currently touch the test table.
func sampleLocks(ctx context.Context, db *pgx.Conn) string {
	q := fmt.Sprintf(`
select coalesce(string_agg(distinct mode, ', '), 'none') as modes
from pg_locks l
join pg_class c on c.oid = l.relation
where c.relname = '%s'`, tableName)
	return queryOneStr(ctx, db, q)
}

// startProbe issues inserts to show how writers block behind table-level locks until explicitly stopped.
func startProbe(ctx context.Context, dsn string) func() {
	pctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		conn := mustConnect(pctx, dsn)
		defer conn.Close(pctx)

		log.Println("[probe] started")
		defer log.Println("[probe] stopping")

		ticker := time.NewTicker(probeTickInterval)
		defer ticker.Stop()

		for i := 0; ; i++ {
			select {
			case <-pctx.Done():
				return
			default:
			}

			start := time.Now()
			_, err := conn.Exec(pctx, fmt.Sprintf(`insert into %s (email) values ('probe-%d-%d@example.com')`, tableName, i, time.Now().UnixNano()))
			label := fmt.Sprintf("#%02d", i+1)
			duration := formatMillis(time.Since(start))

			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				log.Printf("[probe] insert %s failed after %s: %v", label, duration, err)
			} else {
				log.Printf("[probe] insert %s completed after %s", label, duration)
			}

			select {
			case <-pctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

// runNonConcurrentCreate issues the standard IF NOT EXISTS statement with lock logging.
func runNonConcurrentCreate(ctx context.Context, db *pgx.Conn, prefix string) error {
	return runNonConcurrentWithSQL(ctx, db, prefix, fmt.Sprintf(`create index if not exists %s on %s(email)`, indexName, tableName))
}

// runNonConcurrentWithSQL executes an arbitrary non-concurrent index build while logging locks/flags.
func runNonConcurrentWithSQL(ctx context.Context, db *pgx.Conn, prefix, sql string) error {
	log.Printf("%s starting non-concurrent CREATE INDEX IF NOT EXISTS %s", prefix, indexName)
	logLocks(ctx, db, prefix+" locks before non-concurrent build")
	start := time.Now()
	if err := execSQL(ctx, db, sql); err != nil {
		log.Printf("%s non-concurrent build failed after %s: %v", prefix, formatMillis(time.Since(start)), err)
		return err
	}
	log.Printf("%s non-concurrent build finished in %s", prefix, formatMillis(time.Since(start)))
	logLocks(ctx, db, prefix+" locks after non-concurrent build")
	return nil
}

// indexState captures whether the target index exists and its ready/valid flags.
type indexState struct {
	exists bool
	ready  bool
	valid  bool
}

func (s indexState) String() string {
	if !s.exists {
		return fmt.Sprintf("%s=<missing>", indexName)
	}
	return fmt.Sprintf("%s ready=%t valid=%t", indexName, s.ready, s.valid)
}

// fetchIndexState queries pg_index for the given index name.
func fetchIndexState(ctx context.Context, db *pgx.Conn, name string) (indexState, error) {
	const q = `
select i.indisready, i.indisvalid
from pg_index i
join pg_class c on c.oid = i.indexrelid
where c.relname = $1`
	var state indexState
	err := db.QueryRow(ctx, q, name).Scan(&state.ready, &state.valid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return indexState{}, nil
		}
		return indexState{}, err
	}
	state.exists = true
	return state, nil
}

// indexSnapshot records both the catalog state and current CIC progress phase (if any).
type indexSnapshot struct {
	state          indexState
	progressPhase  string
	progressActive bool
}

func (s indexSnapshot) String() string {
	base := s.state.String()
	if !s.progressActive {
		return fmt.Sprintf("%s progress=<inactive>", base)
	}
	return fmt.Sprintf("%s progress=%s", base, s.progressPhase)
}

// fetchIndexSnapshot returns both index catalog flags and any active pg_stat_progress_create_index entry.
func fetchIndexSnapshot(ctx context.Context, db *pgx.Conn) (indexSnapshot, error) {
	state, err := fetchIndexState(ctx, db, indexName)
	if err != nil {
		return indexSnapshot{}, err
	}

	const progressQuery = `
select phase
from pg_stat_progress_create_index
where relid = ($1)::regclass
limit 1`
	var snap indexSnapshot
	snap.state = state

	var phase string
	err = db.QueryRow(ctx, progressQuery, schemaTableName).Scan(&phase)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return snap, nil
		}
		return indexSnapshot{}, err
	}
	snap.progressActive = true
	snap.progressPhase = phase
	return snap, nil
}

// snapshotsEqual reports whether two index snapshots encode the same catalog/progress state.
func snapshotsEqual(a, b indexSnapshot) bool {
	return a.state == b.state &&
		a.progressActive == b.progressActive &&
		a.progressPhase == b.progressPhase
}

// startCICMonitor runs the concurrent index build and streams progress snapshots via cicEvent messages.
func startCICMonitor(ctx context.Context, dsn, cicSQL string) (<-chan cicEvent, <-chan error) {
	events := make(chan cicEvent, 128)
	done := make(chan error, 1)
	errCh := make(chan error, 1)

	go func() {
		conn := mustConnect(ctx, dsn)
		defer conn.Close(ctx)

		log.Println("[cic] started")
		start := time.Now()
		_, err := conn.Exec(ctx, cicSQL)
		duration := time.Since(start)
		if err != nil {
			log.Printf("[cic] failed after %s: %v", formatMillis(duration), err)
		} else {
			log.Printf("[cic] finished in %s", formatMillis(duration))
		}
		errCh <- err
		close(errCh)
	}()

	go func() {
		conn := mustConnect(ctx, dsn)
		defer conn.Close(ctx)
		defer close(events)

		ticker := time.NewTicker(cicPollInterval)
		defer ticker.Stop()

		var (
			last    indexSnapshot
			hasLast bool
		)
		for {
			snap, err := fetchIndexSnapshot(ctx, conn)
			if err != nil {
				select {
				case events <- cicEvent{err: err}:
				case <-ctx.Done():
					return
				}
				continue
			}

			if !hasLast || !snapshotsEqual(last, snap) {
				log.Printf("[cic] snapshot updated: %s", snap)
				last = snap
				hasLast = true
			}

			select {
			case events <- cicEvent{snapshot: snap}:
			case <-ctx.Done():
				return
			}

			select {
			case err, ok := <-errCh:
				if ok {
					done <- err
					close(done)
					if err != nil {
						select {
						case events <- cicEvent{err: err}:
						case <-ctx.Done():
						}
					}
				}
				return
			case <-ctx.Done():
				done <- ctx.Err()
				close(done)
				return
			case <-ticker.C:
			}
		}
	}()

	return events, done
}

// waitForCICEvent consumes cicEvents until the predicate is satisfied or timing out.
func waitForCICEvent(events <-chan cicEvent, timeout time.Duration, predicate func(indexSnapshot) bool) (indexSnapshot, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var last indexSnapshot
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return last, fmt.Errorf("cic events channel closed before predicate satisfied; last=%s", last)
			}
			if ev.err != nil {
				return last, ev.err
			}
			last = ev.snapshot
			if predicate(ev.snapshot) {
				return ev.snapshot, nil
			}
		case <-timer.C:
			return last, fmt.Errorf("timeout waiting for desired CIC state; last=%s", last)
		}
	}
}

// drainCICEvents ensures the monitor channel does not block once we no longer need events.
func drainCICEvents(events <-chan cicEvent) {
	go func() {
		for range events {
		}
	}()
}

// scenarioTimer tracks the start time of the current scenario so logs can emit relative timestamps.
type scenarioTimer struct {
	mu    sync.RWMutex
	start time.Time
}

// Reset records the supplied time as the new reference point for elapsed calculations.
func (t *scenarioTimer) Reset(now time.Time) {
	t.mu.Lock()
	t.start = now
	t.mu.Unlock()
}

// Since reports the duration between the last Reset call and now.
func (t *scenarioTimer) Since(now time.Time) time.Duration {
	t.mu.RLock()
	start := t.start
	t.mu.RUnlock()
	if start.IsZero() {
		return 0
	}
	return now.Sub(start)
}

// relativeWriter prefixes each payload with the scenario-relative elapsed time.
type relativeWriter struct {
	clock *scenarioTimer
	out   io.Writer
}

// Write implements io.Writer by inserting a "[+XX.XXXXs]" prefix before forwarding to the underlying writer.
func (w *relativeWriter) Write(p []byte) (int, error) {
	elapsed := w.clock.Since(time.Now())
	prefix := fmt.Sprintf("[+%06.4fs] ", elapsed.Seconds())
	buf := make([]byte, 0, len(prefix)+len(p))
	buf = append(buf, prefix...)
	buf = append(buf, p...)
	return w.out.Write(buf)
}
