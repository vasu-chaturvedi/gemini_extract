package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/charmbracelet/log"
)

// Job represents a single unit of work: a procedure to be run for a specific SOL ID.
type Job struct {
	SolID string
	Proc  string
}

// worker is a single goroutine that processes jobs from the jobs channel.
func worker(
	id int,
	ctx context.Context,
	wg *sync.WaitGroup,
	runCfg *ExtractionConfig,
	jobs <-chan Job,
	procLogCh chan<- ProcLog,
	summaryMu *sync.Mutex,
	procSummary map[string]ProcSummary,
	stmts map[string]*sql.Stmt,
	slicePool *sync.Pool,
	templates map[string][]ColumnConfig,
	mode string,
) {
	defer wg.Done()
	for job := range jobs {
		start := time.Now()
		var err error

		if mode == "E" {
			log.Debug("Starting extraction", "worker", id, "procedure", job.Proc, "sol_id", job.SolID)
			stmt := stmts[job.Proc]
			err = extractData(ctx, stmt, slicePool, job.Proc, job.SolID, runCfg, templates)
		} else { // mode == "I"
			log.Debug("Starting insertion", "worker", id, "procedure", job.Proc, "sol_id", job.SolID)
			stmt := stmts[runCfg.PackageName+"."+job.Proc]
			err = callProcedure(ctx, stmt, job.SolID)
		}
		end := time.Now()
		duration := end.Sub(start)

		plog := ProcLog{
			SolID:         job.SolID,
			Procedure:     job.Proc,
			StartTime:     start,
			EndTime:       end,
			ExecutionTime: duration,
		}
		if err != nil {
			plog.Status = "FAIL"
			plog.ErrorDetails = err.Error()
			log.Error("Job failed", "worker", id, "procedure", job.Proc, "sol_id", job.SolID, "error", err)
		} else {
			plog.Status = "SUCCESS"
			log.Debug("Job completed", "worker", id, "procedure", job.Proc, "sol_id", job.SolID, "duration", duration.Round(time.Millisecond))
		}
		procLogCh <- plog

		summaryMu.Lock()
		s, exists := procSummary[job.Proc]
		if !exists {
			s = ProcSummary{Procedure: job.Proc, StartTime: start, EndTime: end, Status: plog.Status}
		} else {
			if start.Before(s.StartTime) {
				s.StartTime = start
			}
			if end.After(s.EndTime) {
				s.EndTime = end
			}
			if s.Status != "FAIL" && plog.Status == "FAIL" {
				s.Status = "FAIL"
			}
		}
		procSummary[job.Proc] = s
		summaryMu.Unlock()
	}
}

// prepareStatements creates all the necessary prepared statements before starting the workers.
func prepareStatements(ctx context.Context, db *sql.DB, runCfg *ExtractionConfig, templates map[string][]ColumnConfig, mode string) (map[string]*sql.Stmt, error) {
	stmts := make(map[string]*sql.Stmt)

	for _, proc := range runCfg.Procedures {
		var query, key string

		if mode == "E" {
			cols, ok := templates[proc]
			if !ok {
				return nil, fmt.Errorf("missing template for procedure %s", proc)
			}
			colNames := make([]string, len(cols))
			for i, col := range cols {
				colNames[i] = col.Name
			}
			query = fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", strings.Join(colNames, ", "), proc)
			key = proc
		} else { // mode == "I"
			query = fmt.Sprintf("BEGIN %s.%s(:1); END;", runCfg.PackageName, proc)
			key = runCfg.PackageName + "." + proc
		}

		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			// Close any statements that were successfully created before the error
			for _, s := range stmts {
				s.Close()
			}
			return nil, fmt.Errorf("failed to prepare statement for %s: %w", key, err)
		}
		stmts[key] = stmt
	}

	return stmts, nil
}
