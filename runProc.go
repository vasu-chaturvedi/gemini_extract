package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

func runProcedure(ctx context.Context, db *sql.DB, proc, solID string, procConfig *ExtractionConfig, logCh chan<- ProcLog, summaryCh chan<- SummaryUpdate) {
	start := time.Now()
	log.Printf("🔁 Inserting: %s.%s for SOL %s", procConfig.PackageName, proc, solID)
	err := callProcedure(ctx, db, procConfig.PackageName, proc, solID)
	end := time.Now()

	plog := ProcLog{
		SolID:         solID,
		Procedure:     proc,
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
	logCh <- plog

	summaryCh <- SummaryUpdate{
		Procedure: proc,
		StartTime: start,
		EndTime:   end,
		Status:    plog.Status,
	}
}

func callProcedure(ctx context.Context, db *sql.DB, pkgName, procName, solID string) error {
	query := fmt.Sprintf("BEGIN %s.%s(:1); END;", pkgName, procName)
	start := time.Now()
	_, err := db.ExecContext(ctx, query, solID)
	log.Printf("✅ Finished: %s.%s for SOL %s in %s", pkgName, procName, solID, time.Since(start).Round(time.Millisecond))
	return err
}
