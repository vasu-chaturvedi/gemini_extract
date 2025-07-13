package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

func callProcedureJob(ctx context.Context, db *sql.DB, pkgName, procName, solID string) (ProcLog, error) {
	start := time.Now()
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", pkgName, procName)
	_, err := db.ExecContext(ctx, query, solID)
	end := time.Now()
	log.Printf("✅ Finished: %s.%s for SOL %s in %s", pkgName, procName, solID, end.Sub(start).Round(time.Millisecond))

	plog := ProcLog{
		SolID:         solID,
		Procedure:     procName,
		StartTime:     start,
		EndTime:       end,
		ExecutionTime: end.Sub(start),
	}
	if err != nil {
		plog.Status = "FAIL"
		plog.ErrorDetails = err.Error()
	} else {
		plog.Status = "SUCCESS"
	}
	return plog, err
}
