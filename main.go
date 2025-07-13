package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/godror/godror"
)

var (
	appCfgFile = new(string)
	runCfgFile = new(string)
	mode       string
)




func init() {
	flag.StringVar(appCfgFile, "appCfg", "", "Path to the main application configuration file")
	flag.StringVar(runCfgFile, "runCfg", "", "Path to the extraction configuration file")
	flag.StringVar(&mode, "mode", "", "Mode of operation: E - Extract, I - Insert")
	flag.Parse()

	if mode != "E" && mode != "I" {
		log.Fatal("Invalid mode. Valid values are 'E' for Extract and 'I' for Insert.")
	}
	if *appCfgFile == "" || *runCfgFile == "" {
		log.Fatal("Both appCfg and runCfg must be specified")
	}
	if _, err := os.Stat(*appCfgFile); os.IsNotExist(err) {
		log.Fatalf("Application configuration file does not exist: %s", *appCfgFile)
	}
	if _, err := os.Stat(*runCfgFile); os.IsNotExist(err) {
		log.Fatalf("Extraction configuration file does not exist: %s", *runCfgFile)
	}
}

func main() {
	appCfg, err := loadMainConfig(*appCfgFile)
	if err != nil {
		log.Fatalf("Failed to load main config: %v", err)
	}
	runCfg, err := loadExtractionConfig(*runCfgFile)
	if err != nil {
		log.Fatalf("Failed to load extraction config: %v", err)
	}

	templates := make(map[string][]ColumnConfig)
	if mode == "E" {
		for _, proc := range runCfg.Procedures {
			tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
			cols, err := readColumnsFromCSV(tmplPath)
			if err != nil {
				log.Fatalf("Failed to read template for %s: %v", proc, err)
			}
			templates[proc] = cols
		}
	}

	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(appCfg.Concurrency)
	db.SetMaxIdleConns(appCfg.Concurrency)
	db.SetConnMaxLifetime(30 * time.Minute)

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	ctx := context.Background()

	// --- Prepare statements once if in Extract mode ---
	preparedStmts := make(map[string]*sql.Stmt)
	if mode == "E" {
		log.Println("⚙️ Preparing database statements for extraction...")
		for _, proc := range runCfg.Procedures {
			cols := templates[proc]
			colNames := make([]string, len(cols))
			for i, col := range cols {
				colNames[i] = col.Name
			}
			query := fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", strings.Join(colNames, ", "), proc)

			stmt, err := db.PrepareContext(ctx, query)
			if err != nil {
				log.Fatalf("Failed to prepare statement for %s: %v", proc, err)
			}
			preparedStmts[proc] = stmt
		}
		// Defer closing of all prepared statements
		defer func() {
			for _, stmt := range preparedStmts {
				stmt.Close()
			}
		}()
	}

	procLogCh := make(chan ProcLog, 1000)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}

	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

	totalItems := int64(len(sols) * len(runCfg.Procedures))
	workItemQueue := make(chan WorkItem, totalItems)

	log.Printf("📦 Populating work item queue with %d items...", totalItems)
	for _, sol := range sols {
		for _, proc := range runCfg.Procedures {
			workItemQueue <- WorkItem{SolID: sol, Procedure: proc}
		}
	}
	close(workItemQueue)

	var wg sync.WaitGroup
	overallStart := time.Now()
	var completed int64

	log.Printf("⚙️ Starting %d workers...", appCfg.Concurrency)
	for i := 0; i < appCfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workItemQueue {
				start := time.Now()
				var err error

				if mode == "E" {
					log.Printf("📥 Extracting %s for SOL %s", item.Procedure, item.SolID)
					stmt := preparedStmts[item.Procedure]
					err = extractData(ctx, stmt, item.Procedure, item.SolID, &runCfg, templates[item.Procedure])
				} else if mode == "I" {
					log.Printf("🔁 Inserting: %s.%s for SOL %s", runCfg.PackageName, item.Procedure, item.SolID)
					err = callProcedure(ctx, db, runCfg.PackageName, item.Procedure, item.SolID)
				}
				end := time.Now()

				plog := ProcLog{
					SolID:         item.SolID,
					Procedure:     item.Procedure,
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
				procLogCh <- plog

				summaryMu.Lock()
				s, exists := procSummary[item.Procedure]
				if !exists {
					s = ProcSummary{Procedure: item.Procedure, StartTime: start, EndTime: end, Status: plog.Status}
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
				procSummary[item.Procedure] = s
				summaryMu.Unlock()

				atomic.AddInt64(&completed, 1)
				currentCompleted := atomic.LoadInt64(&completed)
				if currentCompleted%100 == 0 || currentCompleted == totalItems {
					elapsed := time.Since(overallStart)
					estimatedTotal := time.Duration(float64(elapsed) / float64(currentCompleted) * float64(totalItems))
					eta := estimatedTotal - elapsed
					log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
						currentCompleted, totalItems, float64(currentCompleted)*100/float64(totalItems),
						elapsed.Round(time.Second), eta.Round(time.Second))
				}
			}
		}()
	}

	wg.Wait()
	close(procLogCh)

	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
	if mode == "E" {
		mergeFiles(&runCfg)
	}
	log.Printf("🎯 All done! Processed %d SOLs across %d procedures in %s", len(sols), len(runCfg.Procedures), time.Since(overallStart).Round(time.Second))
}
