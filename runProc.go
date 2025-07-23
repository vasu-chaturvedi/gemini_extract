package main

import (
	"context"
	"database/sql"
	"fmt"
)

// callProcedure executes a prepared statement for a given SOL ID.
// It no longer contains logging, as that is handled by the worker function
// which has more context.
func callProcedure(ctx context.Context, stmt *sql.Stmt, solID string) error {
	_, err := stmt.ExecContext(ctx, solID)
	if err != nil {
		return fmt.Errorf("prepared statement execution failed: %w", err)
	}
	return err
}
