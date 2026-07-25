// Package sqliteext provides the storage.sqlite capability for Pulp
// cells, backed by modernc.org/sqlite (pure Go, no CGO).
//
// Deployment:
//
//	import _ "github.com/BananaLabs-OSS/Pulp-ext-sqlite"
//
// Host imports exposed:
//
//	sqlite_exec(query_ptr, query_len, params_ptr, params_len, res_ptr_out, res_len_out) -> error_code
//	sqlite_query(query_ptr, query_len, params_ptr, params_len, rows_ptr_out, rows_len_out) -> error_code
//
// Each declaring cell gets its own database file at
// <storage-root>/<cell-name>/data.db. State is keyed by cell name at
// Register time so multi-cell deployments don't share a connection.
package sqliteext

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
	_ "modernc.org/sqlite"
)

// manager owns per-cell *sql.DB handles. Setup runs once (cell name is
// empty there — see Pulp run.Main) so the DBs are opened lazily on
// first Register with the manifest's cell name baked in.
type sqliteManager struct {
	mu    sync.RWMutex
	dbs   map[ext.ResourceKey]*sql.DB
	slots *ext.ScopedFactory[*sqliteSlot]
	// setups is keyed by application placement, rather than being process
	// global. One host loads this extension for every application it
	// supervises, so a later Setup must never redirect an already-bound cell.
	setups      map[sqliteApplicationKey]sqliteSetup
	storageRoot string
	logger      *slog.Logger
}

type sqliteApplicationKey struct {
	applicationID string
	instanceID    string
}

type sqliteSetup struct {
	storageRoot string
	logger      *slog.Logger
}

// sqliteSlot is retained by ext.ScopedFactory for one ResourceKey. Closing a
// database clears the slot, allowing the same application/cell instance to
// restart with a fresh *sql.DB while preserving the ownership namespace.
type sqliteSlot struct {
	mu sync.Mutex
	db *sql.DB
}

func newSQLiteManager() *sqliteManager {
	return &sqliteManager{
		dbs:    map[ext.ResourceKey]*sql.DB{},
		setups: map[sqliteApplicationKey]sqliteSetup{},
		slots: ext.NewScopedFactory(func(ext.ResourceKey) (*sqliteSlot, error) {
			return &sqliteSlot{}, nil
		}),
	}
}

var manager = newSQLiteManager()

func init() {
	ext.Register(ext.Capability{
		Name:          "storage.sqlite",
		Setup:         setup,
		Teardown:      teardown,
		TeardownScope: teardownScope,
		Register:      bindActive,
		Stub:          bindStub,
		TeardownCell:  teardownCell,
	})
}

// setup captures the storage root and logger. It does NOT open the
// database — Pulp calls Setup once with an empty CellName, so opening
// here would collide across cells. Databases are opened lazily from
// Register() once the cell identity is known.
func setup(env ext.SetupEnv) error {
	return manager.setup(env)
}

func (m *sqliteManager) setup(env ext.SetupEnv) error {
	scope := env.EffectiveScope()
	if err := validateFilesystemScope(scope); err != nil {
		return err
	}
	logger := env.Logger
	if logger == nil {
		logger = slog.Default()
	}
	appKey := sqliteApplicationScopeKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.setups[appKey]; ok {
		if existing.storageRoot != env.StorageRoot {
			return fmt.Errorf("storage.sqlite: application %s/%s setup already owns storage root %q; refusing replacement with %q", appKey.applicationID, appKey.instanceID, existing.storageRoot, env.StorageRoot)
		}
		return nil
	}
	m.setups[appKey] = sqliteSetup{storageRoot: env.StorageRoot, logger: logger}
	if isLegacyScope(scope) {
		m.storageRoot = env.StorageRoot
		m.logger = logger
	}
	if m.logger == nil {
		m.logger = logger
	}
	logger.Info("storage.sqlite setup", "storage_root", env.StorageRoot, "application", appKey.applicationID, "instance", appKey.instanceID)
	return nil
}

func sqliteApplicationScopeKey(scope ext.Scope) sqliteApplicationKey {
	return sqliteApplicationKey{applicationID: scope.ApplicationID(), instanceID: scope.ApplicationInstanceID()}
}

// teardown is the legacy, process-level cleanup path. It closes only legacy
// once — closed handles are removed from the map.
func teardown(_ context.Context) error {
	return manager.teardown()
}

// teardown is only used by legacy hosts. Pulp's legacy Teardown callback has
// no application identity, so it cannot safely release scoped resources in a
// multi-app process. Scoped hosts call teardownScope instead.
func (m *sqliteManager) teardown() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for name, db := range m.dbs {
		if !isLegacyScope(name.Scope()) {
			continue
		}
		if err := db.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", name, err)
		}
		if slot, _, err := m.slots.GetOrCreate(name); err == nil {
			slot.mu.Lock()
			slot.db = nil
			slot.mu.Unlock()
		}
		delete(m.dbs, name)
	}
	// A legacy Setup is one process-wide ownership lease. Once its Teardown
	// runs, a later single-app host/test may legitimately claim a new root.
	// Explicit application setup entries remain untouched.
	delete(m.setups, sqliteApplicationScopeKey(ext.LegacyScope("default")))
	m.storageRoot = ""
	return first
}

// teardownScope closes all databases owned by one application placement. It
// intentionally includes every cell instance under that placement and leaves
// every other application's handles and setup root untouched.
func teardownScope(_ context.Context, scope ext.Scope) error {
	return manager.teardownScope(scope)
}

func (m *sqliteManager) teardownScope(scope ext.Scope) error {
	if err := validateFilesystemScope(scope); err != nil {
		return err
	}
	owner := sqliteApplicationScopeKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for key, db := range m.dbs {
		if sqliteApplicationScopeKey(key.Scope()) != owner {
			continue
		}
		delete(m.dbs, key)
		if slot, _, err := m.slots.GetOrCreate(key); err == nil {
			slot.mu.Lock()
			slot.db = nil
			slot.mu.Unlock()
		}
		if err := db.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", key, err)
		}
	}
	delete(m.setups, owner)
	return first
}

// teardownCell closes just one cell's database during a per-cell
// control-socket shutdown, leaving other cells untouched.
func teardownCell(_ context.Context, cellID string) error {
	return manager.closeCellTarget(cellID)
}

// closeCellTarget accepts both the stable legacy cell name and a scoped
// RoutingID supplied by modern Pulp runtimes. RoutingID is matched against
// already-owned scopes rather than parsed, so a guest/control caller cannot
// manufacture a scope that aliases another application's handle.
func (m *sqliteManager) closeCellTarget(cellID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, db := range m.dbs {
		if !isLegacyScope(key.Scope()) && key.Scope().RoutingID() == cellID {
			return m.closeKeyLocked(key, db)
		}
	}
	key, err := sqliteKey(ext.LegacyScope(cellID))
	if err != nil {
		return err
	}
	db, ok := m.dbs[key]
	if !ok {
		return nil
	}
	return m.closeKeyLocked(key, db)
}

// closeScope releases exactly one application/cell instance database. It is
// kept separate from TeardownCell so existing Pulp hosts retain their
// cell-name-only shutdown behavior while scoped hosts can close precisely.
func (m *sqliteManager) closeScope(scope ext.Scope) error {
	key, err := sqliteKey(scope)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	db, ok := m.dbs[key]
	if !ok {
		return nil
	}
	return m.closeKeyLocked(key, db)
}

func (m *sqliteManager) closeKeyLocked(key ext.ResourceKey, db *sql.DB) error {
	delete(m.dbs, key)
	if slot, _, err := m.slots.GetOrCreate(key); err == nil {
		slot.mu.Lock()
		slot.db = nil
		slot.mu.Unlock()
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close %s: %w", key, err)
	}
	if logger := m.loggerForScopeLocked(key.Scope()); logger != nil {
		logger.Info("storage.sqlite teardown scope", "scope", key)
	}
	return nil
}

// openForCell opens <storage-root>/<cellID>/data.db, applies the
// standard pragmas, and caches the handle. Idempotent — returns the
// cached *sql.DB on subsequent calls.
func (m *sqliteManager) openForCell(cellID string) (*sql.DB, error) {
	return m.openForScope(ext.LegacyScope(cellID))
}

// openForScope opens the database owned by scope and caches it by the complete
// application/cell-instance identity. Validation runs before any filesystem
// operation, so malformed manifest values cannot traverse the storage root or
// collide with another application's state.
func (m *sqliteManager) openForScope(scope ext.Scope) (*sql.DB, error) {
	key, err := sqliteKey(scope)
	if err != nil {
		return nil, err
	}
	slot, _, err := m.slots.GetOrCreate(key)
	if err != nil {
		return nil, fmt.Errorf("storage.sqlite: allocate scoped handle: %w", err)
	}
	m.mu.RLock()
	if db, ok := m.dbs[key]; ok {
		m.mu.RUnlock()
		return db, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check under the write lock — another caller may have raced us.
	if db, ok := m.dbs[key]; ok {
		return db, nil
	}
	storageRoot := m.storageRootForScopeLocked(scope)
	dbPath, err := sqlitePath(storageRoot, scope)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		// Checkpoint the WAL into the main DB file after (essentially) every
		// commit. Without this, low-write cell DBs never reach SQLite's default
		// 1000-page auto-checkpoint threshold, so all data lives only in the
		// -wal sidecar until db.Close() — which the host's window-close
		// os.Exit(0) skips, silently losing data on the next run. Cost is
		// negligible for these small stores.
		"PRAGMA wal_autocheckpoint=1",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	m.dbs[key] = db
	slot.mu.Lock()
	slot.db = db
	slot.mu.Unlock()
	if logger := m.loggerForScopeLocked(scope); logger != nil {
		logger.Info("storage.sqlite ready", "scope", key, "path", dbPath)
	}
	return db, nil
}

// storageRootForScopeLocked selects the immutable root captured by Setup for
// this application placement. The legacy field is a compatibility fallback
// for old hosts and direct package users that do not provide explicit scopes.
// Caller must hold m.mu for reading or writing.
func (m *sqliteManager) storageRootForScopeLocked(scope ext.Scope) string {
	if setup, ok := m.setups[sqliteApplicationScopeKey(scope)]; ok {
		return setup.storageRoot
	}
	return m.storageRoot
}

// loggerForScopeLocked mirrors storageRootForScopeLocked so concurrent app
// setup cannot alter which logger observes a scoped database lifecycle.
// Caller must hold m.mu for reading or writing.
func (m *sqliteManager) loggerForScopeLocked(scope ext.Scope) *slog.Logger {
	if setup, ok := m.setups[sqliteApplicationScopeKey(scope)]; ok {
		return setup.logger
	}
	return m.logger
}

// openForSharedNamespace is the sole opt-in path for cross-application SQLite
// state. Normal capability binding always uses openForScope, so same-named
// cells stay isolated unless a host composition deliberately invokes this
// declared shared-namespace policy.
func (m *sqliteManager) openForSharedNamespace(scope ext.Scope, namespace string) (*sql.DB, error) {
	shared, err := sharedScope(scope, namespace)
	if err != nil {
		return nil, err
	}
	return m.openForScope(shared)
}

func (m *sqliteManager) get(cellID string) (*sql.DB, bool) {
	return m.getForScope(ext.LegacyScope(cellID))
}

// getForScope returns a handle only when the exact validated scope was opened.
// Bound WASM imports capture this scope, so a guest has no runtime selector for
// another application or cell instance.
func (m *sqliteManager) getForScope(scope ext.Scope) (*sql.DB, bool) {
	key, err := sqliteKey(scope)
	if err != nil {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	db, ok := m.dbs[key]
	return db, ok
}

type ExecResult struct {
	RowsAffected int64  `msgpack:"rows_affected"`
	LastInsertID int64  `msgpack:"last_insert_id"`
	Error        string `msgpack:"error,omitempty"`
}

type QueryResult struct {
	Columns []string `msgpack:"columns"`
	Rows    [][]any  `msgpack:"rows"`
	Error   string   `msgpack:"error,omitempty"`
}

// ---- binding ---------------------------------------------------------------

func bindActive(b wazero.HostModuleBuilder, cell ext.Cell) error {
	scope, err := ext.ValidatedScopeOf(cell)
	if err != nil {
		return fmt.Errorf("storage.sqlite: resolve cell scope: %w", err)
	}
	// Open eagerly so a misconfigured storage root fails at cell load,
	// not on the first query. Errors here abort cell registration.
	if _, err := manager.openForScope(scope); err != nil {
		return fmt.Errorf("open sqlite for scope %q: %w", scope.RoutingID(), err)
	}
	exec := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
		return sqliteExec(ctx, m, scope, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut)
	}
	query := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
		return sqliteQuery(ctx, m, scope, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut)
	}
	b.NewFunctionBuilder().WithFunc(exec).Export("sqlite_exec")
	b.NewFunctionBuilder().WithFunc(query).Export("sqlite_query")
	return nil
}

func bindStub(b wazero.HostModuleBuilder, _ ext.Cell) error {
	nop6 := func(_ context.Context, _ api.Module, _, _, _, _, _, _ uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop6).Export("sqlite_exec")
	b.NewFunctionBuilder().WithFunc(nop6).Export("sqlite_query")
	return nil
}

// ---- handlers --------------------------------------------------------------

func sqliteExec(ctx context.Context, m api.Module, scope ext.Scope, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
	if qLen == 0 {
		return 1
	}
	q, ok := m.Memory().Read(qPtr, qLen)
	if !ok {
		return 2
	}
	args, code := decodeArgs(m, pPtr, pLen)
	if code != 0 {
		return code
	}
	db, ok := manager.getForScope(scope)
	if !ok {
		return 9
	}
	res, err := db.ExecContext(ctx, string(q), args...)
	if err != nil {
		encoded, mErr := msgpack.Marshal(ExecResult{Error: err.Error()})
		if mErr != nil {
			return 5
		}
		_ = writeResponse(ctx, m, encoded, resPtrOut, resLenOut)
		return execErrorCode(err)
	}
	var out ExecResult
	if ra, raErr := res.RowsAffected(); raErr != nil {
		manager.logger.Warn("sqlite: RowsAffected failed", "err", raErr)
	} else {
		out.RowsAffected = ra
	}
	if lid, lidErr := res.LastInsertId(); lidErr != nil {
		manager.logger.Warn("sqlite: LastInsertId failed", "err", lidErr)
	} else {
		out.LastInsertID = lid
	}
	encoded, err := msgpack.Marshal(out)
	if err != nil {
		return 5
	}
	return writeResponse(ctx, m, encoded, resPtrOut, resLenOut)
}

func sqliteQuery(ctx context.Context, m api.Module, scope ext.Scope, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
	if qLen == 0 {
		return 1
	}
	q, ok := m.Memory().Read(qPtr, qLen)
	if !ok {
		return 2
	}
	args, code := decodeArgs(m, pPtr, pLen)
	if code != 0 {
		return code
	}
	db, ok := manager.getForScope(scope)
	if !ok {
		return 9
	}
	rows, err := db.QueryContext(ctx, string(q), args...)
	if err != nil {
		return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
	}
	result := QueryResult{Columns: cols}
	for rows.Next() {
		values := make([]any, len(cols))
		scan := make([]any, len(cols))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
		}
		result.Rows = append(result.Rows, values)
	}
	if err := rows.Err(); err != nil {
		return writeQueryError(ctx, m, err, rowsPtrOut, rowsLenOut)
	}
	encoded, err := msgpack.Marshal(result)
	if err != nil {
		return 5
	}
	return writeResponse(ctx, m, encoded, rowsPtrOut, rowsLenOut)
}

func writeQueryError(ctx context.Context, m api.Module, err error, ptrOut, lenOut uint32) uint32 {
	encoded, mErr := msgpack.Marshal(QueryResult{Error: err.Error()})
	if mErr != nil {
		return 5
	}
	_ = writeResponse(ctx, m, encoded, ptrOut, lenOut)
	return execErrorCode(err)
}

// execErrorCode maps sqlite errors to a coarse host code so cells can
// branch on busy vs constraint vs generic without parsing the message.
// 5 = generic, 12 = busy/locked, 13 = constraint violation, 14 = readonly.
func execErrorCode(err error) uint32 {
	if err == nil {
		return 0
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "SQLITE_BUSY"),
		strings.Contains(msg, "database is locked"),
		strings.Contains(msg, "SQLITE_LOCKED"):
		return 12
	case strings.Contains(msg, "SQLITE_CONSTRAINT"),
		strings.Contains(msg, "UNIQUE constraint"),
		strings.Contains(msg, "FOREIGN KEY constraint"),
		strings.Contains(msg, "NOT NULL constraint"),
		strings.Contains(msg, "CHECK constraint"):
		return 13
	case strings.Contains(msg, "SQLITE_READONLY"),
		strings.Contains(msg, "attempt to write a readonly database"):
		return 14
	default:
		return 5
	}
}

// ---- helpers ---------------------------------------------------------------

func decodeArgs(m api.Module, ptr, ln uint32) ([]any, uint32) {
	if ln == 0 {
		return nil, 0
	}
	data, ok := m.Memory().Read(ptr, ln)
	if !ok {
		return nil, 2
	}
	var args []any
	if err := msgpack.Unmarshal(data, &args); err != nil {
		return nil, 3
	}
	return args, 0
}

func writeResponse(ctx context.Context, m api.Module, data []byte, ptrOut, lenOut uint32) uint32 {
	allocFn := m.ExportedFunction("pulp_alloc")
	if allocFn == nil {
		return 7
	}
	var ptr uint32
	if len(data) > 0 {
		results, err := allocFn.Call(ctx, uint64(len(data)))
		if err != nil || len(results) == 0 {
			return 7
		}
		ptr = uint32(results[0])
		if ptr == 0 {
			return 7
		}
		if !m.Memory().Write(ptr, data) {
			return 8
		}
	}
	if !m.Memory().WriteUint32Le(ptrOut, ptr) {
		return 8
	}
	if !m.Memory().WriteUint32Le(lenOut, uint32(len(data))) {
		return 8
	}
	return 0
}
