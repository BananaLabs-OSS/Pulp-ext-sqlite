package sqliteext

import (
	"errors"
	"strings"
	"testing"
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
