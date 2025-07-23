package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/charmbracelet/log"
	_ "github.com/godror/godror"
)

var (
	appCfgFile = flag.String("appCfg", "", "Path to the main application configuration file")
	runCfgFile = flag.String("runCfg", "", "Path to the extraction configuration file")
	mode       = flag.String("mode", "", "Mode of operation: E - Extract, I - Insert")
)

func main() {
	flag.Parse()

	// Centralized error handling
	if err := run(); err != nil {
		log.Fatalf("❌ Application failed: %v", err)
	}
}

// run is the main application logic, designed to return errors for graceful handling.
func run() error {
	// --- Configuration and Validation ---
	if *mode != "E" && *mode != "I" {
		return fmt.Errorf("invalid mode: must be 'E' for Extract or 'I' for Insert")
	}
	if *appCfgFile == "" || *runCfgFile == "" {
		return fmt.Errorf("both appCfg and runCfg flags must be specified")
	}
	for _, path := range []string{*appCfgFile, *runCfgFile} {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return fmt.Errorf("configuration file does not exist: %s", path)
		}
	}

	log.Info("🚀 Starting application...")

	appCfg, err := loadConfig[MainConfig](*appCfgFile)
	if err != nil {
		return fmt.Errorf("failed to load main config: %w", err)
	}
	runCfg, err := loadConfig[ExtractionConfig](*runCfgFile)
	if err != nil {
		return fmt.Errorf("failed to load extraction config: %w", err)
	}

	// --- Database and Template Setup ---
	templates := make(map[string][]ColumnConfig)
	if *mode == "E" {
		log.Info("Loading extraction templates...")
		for _, proc := range runCfg.Procedures {
			tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
			cols, err := readColumnsFromCSV(tmplPath)
			if err != nil {
				return fmt.Errorf("failed to read template for %s: %w", proc, err)
			}
			templates[proc] = cols
		}
	}

	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	db, err := sql.Open("godror", connString)
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(appCfg.Concurrency)
	db.SetMaxIdleConns(appCfg.Concurrency)
	db.SetConnMaxLifetime(30 * time.Minute)

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		return fmt.Errorf("failed to read SOL IDs: %w", err)
	}

	// --- Logging and Concurrency Setup ---
	procLogCh := make(chan ProcLog, 1000)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	if (*mode == "I" && !runCfg.RunInsertionParallel) || (*mode == "E" && !runCfg.RunExtractionParallel) {
		log.Info("Parallel execution disabled, setting concurrency to 1.")
		appCfg.Concurrency = 1
	}

	var logFile, logFileSummary string
	if *mode == "I" {
		logFile = runCfg.PackageName + "_insert.csv"
		logFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else {
		logFile = runCfg.PackageName + "_extract.csv"
		logFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}
	go writeLog(filepath.Join(appCfg.LogFilePath, logFile), procLogCh)

	ctx := context.Background()

	// --- Prepare Statements ---
	log.Info("Preparing database statements...")
	stmts, err := prepareStatements(ctx, db, &runCfg, templates, *mode)
	if err != nil {
		return fmt.Errorf("failed to prepare statements: %w", err)
	}
	defer func() {
		for _, stmt := range stmts {
			stmt.Close()
		}
	}()

	// --- Setup Worker Pool ---
	var wg sync.WaitGroup
	jobs := make(chan Job, 1000)

	maxCols := 0
	if *mode == "E" {
		for _, cols := range templates {
			if len(cols) > maxCols {
				maxCols = len(cols)
			}
		}
	}
	slicePool := &sync.Pool{
		New: func() interface{} {
			return make([]interface{}, maxCols)
		},
	}

	log.Info("Starting worker pool", "concurrency", appCfg.Concurrency)
	for i := 0; i < appCfg.Concurrency; i++ {
		wg.Add(1)
		go worker(i+1, ctx, &wg, &runCfg, jobs, procLogCh, &summaryMu, procSummary, stmts, slicePool, templates, *mode)
	}

	// --- Dispatch Jobs ---
	totalJobs := len(sols) * len(runCfg.Procedures)
	log.Info("Dispatching jobs...", "sols", len(sols), "procedures", len(runCfg.Procedures), "total_jobs", totalJobs)
	overallStart := time.Now()

	go func() {
		for _, sol := range sols {
			for _, proc := range runCfg.Procedures {
				jobs <- Job{SolID: sol, Proc: proc}
			}
		}
		close(jobs)
	}()

	wg.Wait()
	close(procLogCh)

	log.Info("All jobs completed.")

	// --- Finalization ---
	writeSummary(filepath.Join(appCfg.LogFilePath, logFileSummary), procSummary)
	if *mode == "E" {
		if err := mergeFiles(&runCfg); err != nil {
			return fmt.Errorf("failed to merge files: %w", err)
		}
	}
	log.Infof("🎯 All done! Processed %d jobs in %s", totalJobs, time.Since(overallStart).Round(time.Second))
	return nil
}
