// Package migrations holds the SQL files for the game schema. The schema itself
// is created by pgdb.Migrate, so the files here don't write CREATE SCHEMA.
package migrations

import "embed"

// Files holds every *.sql file, run in name order.
//
//go:embed *.sql
var Files embed.FS
