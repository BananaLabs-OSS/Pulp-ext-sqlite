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
	mu          sync.RWMutex
	dbs         map[string]*sql.DB
	storageRoot string
	logger      *slog.Logger
}

var manager = &sqliteManager{dbs: map[string]*sql.DB{}}

func init() {
	ext.Register(ext.Capability{
		Name:         "storage.sqlite",
		Setup:        setup,
		Teardown:     teardown,
		Register:     bindActive,
		Stub:         bindStub,
		TeardownCell: teardownCell,
	})
}

// setup captures the storage root and logger. It does NOT open the
// database — Pulp calls Setup once with an empty CellName, so opening
// here would collide across cells. Databases are opened lazily from
// Register() once the cell identity is known.
func setup(env ext.SetupEnv) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.storageRoot = env.StorageRoot
	manager.logger = env.Logger
	if manager.logger == nil {
		manager.logger = slog.Default()
	}
	manager.logger.Info("storage.sqlite setup", "storage_root", env.StorageRoot)
	return nil
}

// teardown closes every open per-cell database. Safe to call more than
// once — closed handles are removed from the map.
func teardown(_ context.Context) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	var first error
	for name, db := range manager.dbs {
		if err := db.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", name, err)
		}
		delete(manager.dbs, name)
	}
	return first
}

// teardownCell closes just one cell's database during a per-cell
// control-socket shutdown, leaving other cells untouched.
func teardownCell(_ context.Context, cellID string) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	db, ok := manager.dbs[cellID]
	if !ok {
		return nil
	}
	delete(manager.dbs, cellID)
	if err := db.Close(); err != nil {
		return fmt.Errorf("close %s: %w", cellID, err)
	}
	if manager.logger != nil {
		manager.logger.Info("storage.sqlite teardown cell", "cell", cellID)
	}
	return nil
}

// openForCell opens <storage-root>/<cellID>/data.db, applies the
// standard pragmas, and caches the handle. Idempotent — returns the
// cached *sql.DB on subsequent calls.
func (m *sqliteManager) openForCell(cellID string) (*sql.DB, error) {
	m.mu.RLock()
	if db, ok := m.dbs[cellID]; ok {
		m.mu.RUnlock()
		return db, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check under the write lock — another caller may have raced us.
	if db, ok := m.dbs[cellID]; ok {
		return db, nil
	}
	if m.storageRoot == "" {
		return nil, fmt.Errorf("storage.sqlite: setup not called before register")
	}
	dbPath := filepath.Join(m.storageRoot, cellID, "data.db")
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
	m.dbs[cellID] = db
	if m.logger != nil {
		m.logger.Info("storage.sqlite ready", "cell", cellID, "path", dbPath)
	}
	return db, nil
}

func (m *sqliteManager) get(cellID string) (*sql.DB, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	db, ok := m.dbs[cellID]
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
	cellID := cell.Name()
	// Open eagerly so a misconfigured storage root fails at cell load,
	// not on the first query. Errors here abort cell registration.
	if _, err := manager.openForCell(cellID); err != nil {
		return fmt.Errorf("open sqlite for cell %q: %w", cellID, err)
	}
	exec := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
		return sqliteExec(ctx, m, cellID, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut)
	}
	query := func(ctx context.Context, m api.Module, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
		return sqliteQuery(ctx, m, cellID, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut)
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

func sqliteExec(ctx context.Context, m api.Module, cellID string, qPtr, qLen, pPtr, pLen, resPtrOut, resLenOut uint32) uint32 {
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
	db, ok := manager.get(cellID)
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

func sqliteQuery(ctx context.Context, m api.Module, cellID string, qPtr, qLen, pPtr, pLen, rowsPtrOut, rowsLenOut uint32) uint32 {
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
	db, ok := manager.get(cellID)
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
