# Pulp-ext-sqlite

SQLite storage capability for Pulp cells. Pure Go, no CGO — backed by [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite).

From [BananaLabs OSS](https://github.com/BananaLabs-OSS).

## Deployment

```go
import _ "github.com/BananaLabs-OSS/Pulp-ext-sqlite"
```

## Capability

- `storage.sqlite` — `sqlite_exec` and `sqlite_query` host imports, msgpack-encoded params and rows
