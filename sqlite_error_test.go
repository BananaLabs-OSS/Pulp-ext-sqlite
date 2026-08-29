package sqliteext

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSQLiteStatementErrorIncludesBoundedSQLWithoutParameters(t *testing.T) {
	query := []byte("INSERT INTO migrations DEFAULT VALUES" + strings.Repeat("x", 600))
	got := sqliteStatementError(errors.New("syntax error"), query)
	if !strings.Contains(got, `sql="INSERT INTO migrations DEFAULT VALUES`) {
		t.Fatalf("missing SQL context: %q", got)
	}
	if len(got) > 560 || !strings.Contains(got, "...") {
		t.Fatalf("SQL context was not bounded: length=%d", len(got))
	}
}

func TestNormalizeSQLiteQueryMakesBunMigrationInsertDialectSafe(t *testing.T) {
	query := `INSERT INTO bun_migrations ("id", "name", "group_id", "migrated_at") VALUES (DEFAULT, '00000001', 1, DEFAULT) RETURNING "id", "migrated_at"`
	want := `INSERT INTO bun_migrations ("name", "group_id") VALUES ('00000001', 1) RETURNING "id", "migrated_at"`
	if got := normalizeSQLiteQuery(query); got != want {
		t.Fatalf("normalized query = %q, want %q", got, want)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE bun_migrations (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, group_id INTEGER NOT NULL, migrated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	var id int64
	var migratedAt string
	if err := db.QueryRow(want).Scan(&id, &migratedAt); err != nil {
		t.Fatal(err)
	}
	if id != 1 || migratedAt == "" {
		t.Fatalf("defaulted migration row = id %d, migrated_at %q", id, migratedAt)
	}
}
