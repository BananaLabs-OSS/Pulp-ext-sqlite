package sqliteext

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/BananaLabs-OSS/Pulp/ext"
)

const (
	sqliteResourceType = "storage.sqlite"
	sqliteResourceID   = "data.db"
)

// sqliteKey makes the Pulp application/cell placement the sole owner of one
// SQLite handle. ResourceKey is comparable and injective, so application and
// instance boundaries cannot collide in the manager map.
func sqliteKey(scope ext.Scope) (ext.ResourceKey, error) {
	if err := validateFilesystemScope(scope); err != nil {
		return ext.ResourceKey{}, err
	}
	return scope.ResourceKey(sqliteResourceType, sqliteResourceID)
}

// sqlitePath maps a validated Pulp scope to its durable namespace. Existing
// single-app hosts retain their exact historical path; explicit application
// scopes get application and cell-instance directories.
func sqlitePath(storageRoot string, scope ext.Scope) (string, error) {
	if storageRoot == "" {
		return "", fmt.Errorf("storage.sqlite: setup not called before register")
	}
	if err := validateFilesystemScope(scope); err != nil {
		return "", err
	}
	if isLegacyScope(scope) {
		return filepath.Join(storageRoot, scope.CellID(), "data.db"), nil
	}
	return filepath.Join(
		storageRoot,
		"apps", scope.ApplicationID(), scope.ApplicationInstanceID(),
		"cells", scope.CellID(), scope.CellInstanceID(),
		"data.db",
	), nil
}

func isLegacyScope(scope ext.Scope) bool {
	return scope.ApplicationID() == "legacy" &&
		scope.ApplicationInstanceID() == "default" &&
		scope.CellInstanceID() == "default"
}

// validateFilesystemScope layers filesystem-safe components on top of
// ext.Scope's non-empty/NUL validation. Scope metadata originates in manifests;
// it must not turn into path traversal, a volume-qualified path, or aliases.
func validateFilesystemScope(scope ext.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("storage.sqlite: invalid scope: %w", err)
	}
	for _, part := range []struct {
		name  string
		value string
	}{
		{"application ID", scope.ApplicationID()},
		{"application instance ID", scope.ApplicationInstanceID()},
		{"cell ID", scope.CellID()},
		{"cell instance ID", scope.CellInstanceID()},
	} {
		if err := validatePathPart(part.name, part.value); err != nil {
			return err
		}
	}
	return nil
}

func validatePathPart(name, value string) error {
	if value == "." || value == ".." || strings.ContainsAny(value, `\\/:`) || strings.Contains(value, "..") {
		return fmt.Errorf("storage.sqlite: invalid %s %q", name, value)
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			continue
		}
		return fmt.Errorf("storage.sqlite: invalid %s %q", name, value)
	}
	return nil
}

// sharedScope is intentionally not inferred by normal binding. A caller can
// share state only by declaring this explicit policy and using the same
// namespace/cell/cell-instance tuple. The synthetic application identity still
// flows through ext.Scope.ResourceKey, retaining the same collision guarantees.
func sharedScope(scope ext.Scope, namespace string) (ext.Scope, error) {
	if err := validateFilesystemScope(scope); err != nil {
		return ext.Scope{}, err
	}
	if err := validatePathPart("shared namespace", namespace); err != nil {
		return ext.Scope{}, err
	}
	return ext.NewScope("shared."+namespace, "global", scope.CellID(), scope.CellInstanceID())
}
