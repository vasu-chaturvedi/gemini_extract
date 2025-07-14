package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

func callProcedure(ctx context.Context, db *sql.DB, pkgName, procName, solID string) error {
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", pkgName, procName)
	start := time.Now()
	_, err := db.ExecContext(ctx, query, solID)
	log.Printf("✅ Finished: %s.%s for SOL %s in %s", pkgName, procName, solID, time.Since(start).Round(time.Millisecond))
	return err
}