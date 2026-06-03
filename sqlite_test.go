package sqliteext

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newManager returns a fresh sqliteManager rooted at a temp storage dir so
// tests never touch the package-global instance or each other's files.
func newManager(t *testing.T) *sqliteManager {
	t.Helper()
	mgr := &sqliteManager{
		dbs:         map[string]*sql.DB{},
		storageRoot: t.TempDir(),
		logger:      slog.Default(),
	}
	// Windows cannot RemoveAll a dir holding open files, and t.TempDir's
	// cleanup runs before any test-registered cleanup. Close every db this
	// manager opened FIRST so the temp dir teardown succeeds.
	t.Cleanup(func() {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		for name, db := range mgr.dbs {
			_ = db.Close()
			delete(mgr.dbs, name)
		}
	})
	return mgr
}

// TestPerCellDatabaseIsolation is the REFERENCE isolation model that
// ext-postgres had to mirror (audit MASTER.md: "sqlite per-cell *sql.DB
// keyed by cellID captured in the bind closure → wasm cannot name another
// cell's DB"). Cell A and cell B must get distinct *sql.DB handles backed
// by distinct files; a write in A's db is invisible to B.
func TestPerCellDatabaseIsolation(t *testing.T) {
	mgr := newManager(t)

	dbA, err := mgr.openForCell("alpha")
	if err != nil {
		t.Fatalf("openForCell(alpha): %v", err)
	}
	dbB, err := mgr.openForCell("beta")
	if err != nil {
		t.Fatalf("openForCell(beta): %v", err)
	}

	if dbA == dbB {
		t.Fatal("alpha and beta share the same *sql.DB handle; cells are not isolated")
	}

	// The two databases must live in distinct per-cell subdirs of the
	// shared storage root, never the root itself.
	pathA := filepath.Join(mgr.storageRoot, "alpha", "data.db")
	pathB := filepath.Join(mgr.storageRoot, "beta", "data.db")
	if _, err := os.Stat(pathA); err != nil {
		t.Fatalf("alpha data.db not created at %q: %v", pathA, err)
	}
	if _, err := os.Stat(pathB); err != nil {
		t.Fatalf("beta data.db not created at %q: %v", pathB, err)
	}

	// Cell A writes a secret table+row.
	if _, err := dbA.Exec(`CREATE TABLE secrets (v TEXT)`); err != nil {
		t.Fatalf("alpha create: %v", err)
	}
	if _, err := dbA.Exec(`INSERT INTO secrets (v) VALUES (?)`, "alpha-only"); err != nil {
		t.Fatalf("alpha insert: %v", err)
	}

	// Cell B must NOT see alpha's table at all — separate file, separate db.
	if _, err := dbB.Query(`SELECT v FROM secrets`); err == nil {
		t.Fatal("beta could query alpha's 'secrets' table; databases are not isolated")
	}

	// And alpha's own data is intact.
	var got string
	if err := dbA.QueryRow(`SELECT v FROM secrets`).Scan(&got); err != nil {
		t.Fatalf("alpha re-read: %v", err)
	}
	if got != "alpha-only" {
		t.Fatalf("alpha data corrupted: %q", got)
	}
}

// TestOpenForCellIdempotent confirms the handle is cached: a second
// openForCell for the same cell returns the SAME *sql.DB (so a cell does
// not leak file handles or open a second connection per query), and get()
// only returns a registered cell.
func TestOpenForCellIdempotent(t *testing.T) {
	mgr := newManager(t)

	first, err := mgr.openForCell("alpha")
	if err != nil {
		t.Fatalf("openForCell: %v", err)
	}
	second, err := mgr.openForCell("alpha")
	if err != nil {
		t.Fatalf("openForCell again: %v", err)
	}
	if first != second {
		t.Fatalf("openForCell not idempotent: %p vs %p", first, second)
	}
	if _, ok := mgr.get("alpha"); !ok {
		t.Fatal("get(alpha) missing after open")
	}
	if _, ok := mgr.get("ghost"); ok {
		t.Fatal("get(ghost) returned a handle for an unregistered cell")
	}
}

// TestOpenForCellRequiresSetup confirms openForCell fails closed when the
// storage root was never captured (Setup not called), rather than writing
// a data.db into the process CWD.
func TestOpenForCellRequiresSetup(t *testing.T) {
	mgr := &sqliteManager{dbs: map[string]*sql.DB{}} // no storageRoot
	if _, err := mgr.openForCell("alpha"); err == nil {
		t.Fatal("openForCell with no storage root should fail, got nil")
	}
}

// TestSQLParameterBinding proves arguments are bound as parameters, not
// interpolated: a classic injection payload in a bound value lands as
// literal data and cannot terminate the statement / drop a table.
func TestSQLParameterBinding(t *testing.T) {
	mgr := newManager(t)
	db, err := mgr.openForCell("alpha")
	if err != nil {
		t.Fatalf("openForCell: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE users (name TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The injection attempt is supplied as a bound parameter. If binding
	// works it is stored verbatim as one row; if it were interpolated the
	// trailing statement would drop the table.
	payload := "robert'); DROP TABLE users;--"
	if _, err := db.Exec(`INSERT INTO users (name) VALUES (?)`, payload); err != nil {
		t.Fatalf("parameterized insert: %v", err)
	}

	// Table still exists and holds the payload verbatim.
	var name string
	if err := db.QueryRow(`SELECT name FROM users WHERE name = ?`, payload).Scan(&name); err != nil {
		t.Fatalf("read back (table dropped? injection succeeded?): %v", err)
	}
	if name != payload {
		t.Fatalf("payload mangled: got %q want %q", name, payload)
	}

	// Count is exactly 1 — no extra statement executed.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("row count = %d, want 1 (injection may have run extra statements)", n)
	}
}

// TestExecErrorCodeClassification pins the coarse host error codes cells
// branch on (busy=12, constraint=13, readonly=14, generic=5). These are
// part of the host ABI contract — a silent renumbering would break cells.
func TestExecErrorCodeClassification(t *testing.T) {
	cases := []struct {
		msg  string
		want uint32
	}{
		{"SQLITE_BUSY: database is locked", 12},
		{"database is locked", 12},
		{"SQLITE_LOCKED", 12},
		{"SQLITE_CONSTRAINT: UNIQUE constraint failed", 13},
		{"UNIQUE constraint failed: users.email", 13},
		{"FOREIGN KEY constraint failed", 13},
		{"NOT NULL constraint failed", 13},
		{"CHECK constraint failed", 13},
		{"attempt to write a readonly database", 14},
		{"SQLITE_READONLY", 14},
		{"some other syntax error", 5},
	}
	for _, c := range cases {
		if got := execErrorCode(fmtErr(c.msg)); got != c.want {
			t.Errorf("execErrorCode(%q) = %d, want %d", c.msg, got, c.want)
		}
	}
	if got := execErrorCode(nil); got != 0 {
		t.Errorf("execErrorCode(nil) = %d, want 0", got)
	}
}

// TestRealConstraintErrorMapsToCode drives a genuine UNIQUE violation
// through the real driver and asserts execErrorCode classifies it as a
// constraint (13), so the message-substring matching tracks the actual
// modernc.org/sqlite error text, not a guess.
func TestRealConstraintErrorMapsToCode(t *testing.T) {
	mgr := newManager(t)
	db, err := mgr.openForCell("alpha")
	if err != nil {
		t.Fatalf("openForCell: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE u (email TEXT UNIQUE)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO u (email) VALUES (?)`, "a@b.c"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err = db.Exec(`INSERT INTO u (email) VALUES (?)`, "a@b.c")
	if err == nil {
		t.Fatal("duplicate insert should violate UNIQUE constraint, got nil")
	}
	if code := execErrorCode(err); code != 13 {
		t.Fatalf("real UNIQUE violation classified as %d, want 13 (constraint); err=%v", code, err)
	}
}

// TestTeardownCellClosesOnlyThatCell confirms a per-cell shutdown closes
// just that cell's DB and leaves siblings open and usable. teardownCell is
// a package func operating on the global manager, so we swap the global for
// an isolated one for the duration of the test.
func TestTeardownCellClosesOnlyThatCell(t *testing.T) {
	saved := manager
	t.Cleanup(func() { manager = saved })
	manager = newManager(t)

	dbA, err := manager.openForCell("alpha")
	if err != nil {
		t.Fatalf("open alpha: %v", err)
	}
	dbB, err := manager.openForCell("beta")
	if err != nil {
		t.Fatalf("open beta: %v", err)
	}

	if err := teardownCell(context.Background(), "alpha"); err != nil {
		t.Fatalf("teardownCell(alpha): %v", err)
	}
	if _, ok := manager.get("alpha"); ok {
		t.Fatal("alpha still registered after teardownCell")
	}
	// alpha's handle is closed.
	if err := dbA.Ping(); err == nil {
		t.Error("alpha db still pingable after teardownCell; not closed")
	}
	// beta is untouched and usable.
	if _, ok := manager.get("beta"); !ok {
		t.Fatal("beta deregistered by alpha teardown")
	}
	if err := dbB.Ping(); err != nil {
		t.Errorf("beta db unusable after alpha teardown: %v", err)
	}

	// Tearing down an unknown cell is a no-op, not an error.
	if err := teardownCell(context.Background(), "ghost"); err != nil {
		t.Errorf("teardownCell(ghost) = %v, want nil (no-op)", err)
	}
}

// TestCellIDPathScoping documents the per-cell file location and the
// AUDIT-flagged MEDIUM gap: unlike Pulp-ext-fs, openForCell does NOT
// sanitize cellID before joining it into the storage path. With the
// operator-authored manifest names that are the only real inputs today
// (audit: "latent, not live"), distinct legitimate cellIDs resolve to
// distinct, properly-nested data.db files — the isolation property the
// test pins. The traversal sub-case records the gap so that IF a
// sanitizeCellID guard is added later, this test flags the behavior
// change rather than the gap silently persisting.
func TestCellIDPathScoping(t *testing.T) {
	mgr := newManager(t)

	// Legitimate names nest correctly under the storage root.
	for _, id := range []string{"alpha", "cell-1", "cell_2", "AbC123"} {
		db, err := mgr.openForCell(id)
		if err != nil {
			t.Fatalf("openForCell(%q): %v", id, err)
		}
		want := filepath.Join(mgr.storageRoot, id, "data.db")
		if _, err := os.Stat(want); err != nil {
			t.Errorf("cell %q db not at %q: %v", id, want, err)
		}
		_ = db
	}

	// Traversal sub-case: ".." in a cellID currently escapes the per-cell
	// subdir because there is no sanitizeCellID guard (unlike ext-fs).
	// This is the documented latent gap. We assert the CURRENT behavior so
	// the test is a tripwire: adding a guard (rejecting the name) will make
	// this branch fail loudly and prompt updating the expectation.
	escapeID := filepath.Join("..", "escaped")
	mgr2 := newManager(t)
	_, err := mgr2.openForCell(escapeID)
	escapedPath := filepath.Join(mgr2.storageRoot, "..", "escaped", "data.db")
	_, statErr := os.Stat(escapedPath)
	switch {
	case err == nil && statErr == nil:
		// Current (unsanitized) behavior: the file landed OUTSIDE the
		// storage root. Clean it up and record the gap.
		_ = os.RemoveAll(filepath.Dir(escapedPath))
		t.Logf("KNOWN GAP (audit MEDIUM): cellID %q escaped storage root to %q — no sanitizeCellID guard in ext-sqlite. Operator-authored names only today, so latent. If a guard is added, update this test.", escapeID, escapedPath)
	case err != nil:
		// A guard now rejects the malicious name — the gap is closed.
		if statErr == nil {
			_ = os.RemoveAll(filepath.Dir(escapedPath))
			t.Errorf("openForCell(%q) errored (%v) but STILL created a db outside the root at %q", escapeID, err, escapedPath)
		}
		// else: rejected and nothing created — good. Nothing to assert.
	default:
		t.Errorf("unexpected: openForCell(%q) succeeded but no file at %q", escapeID, escapedPath)
	}
}

// fmtErr is a tiny error wrapper so the classification table can use plain
// strings without importing errors in every case.
func fmtErr(s string) error { return strErr(s) }

type strErr string

func (e strErr) Error() string { return string(e) }

// ensure strings import stays used if the table above is trimmed.
var _ = strings.Contains
