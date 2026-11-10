// Package pgdb opens the Postgres pool and runs migrations per service schema.
package pgdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Table returns the quoted, schema-qualified table name, for example "game"."outbox".
func Table(schema, name string) string { return pgx.Identifier{schema, name}.Sanitize() }

// IsUniqueViolation reports whether err is a unique_violation (SQLSTATE 23505).
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Open opens the pool and waits up to 60 seconds for Postgres to be ready. Waiting
// is an infrastructure concern, so it uses the system clock directly, not Clock.
func Open(ctx context.Context, url string, log *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("invalid DATABASE_URL: %w", err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("postgres is unreachable: %w", err)
		}
		log.Info("waiting for postgres", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Migrate runs the *.sql files from files in name order, each in its own
// transaction. Applied versions are recorded in <schema>.schema_migrations, and
// an advisory lock keeps two replicas from migrating at the same time. The
// schema is created here, so migration files don't need CREATE SCHEMA.
func Migrate(ctx context.Context, pool *pgxpool.Pool, schema string, files fs.FS) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	lock := "migrate:" + schema
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtext($1))", lock); err != nil {
		return fmt.Errorf("migration advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock(hashtext($1))", lock)
	}()

	versions := Table(schema, "schema_migrations")
	setup := fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s; CREATE TABLE IF NOT EXISTS %s (version text PRIMARY KEY, applied_at timestamptz NOT NULL)",
		pgx.Identifier{schema}.Sanitize(), versions)
	if err := conn.Conn().PgConn().Exec(ctx, setup).Close(); err != nil {
		return fmt.Errorf("preparing schema %s: %w", schema, err)
	}

	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		var done bool
		q := "SELECT EXISTS (SELECT 1 FROM " + versions + " WHERE version = $1)"
		if err := conn.QueryRow(ctx, q, name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		body, err := fs.ReadFile(files, name)
		if err != nil {
			return err
		}
		if err := apply(ctx, conn.Conn(), versions, name, string(body)); err != nil {
			return fmt.Errorf("migration %s/%s: %w", schema, name, err)
		}
	}
	return nil
}

func apply(ctx context.Context, conn *pgx.Conn, versions, name, body string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// pgconn.Exec uses the simple protocol, so one file may contain many statements.
	if err := tx.Conn().PgConn().Exec(ctx, body).Close(); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "INSERT INTO "+versions+" (version, applied_at) VALUES ($1, $2)", name, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
