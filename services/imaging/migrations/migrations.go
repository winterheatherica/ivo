// Package migrations holds the SQL files for the imaging schema.
package migrations

import "embed"

// Files holds every *.sql file, run in name order.
//
//go:embed *.sql
var Files embed.FS
