package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
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

	// Load templates
	templates := make(map[string][]ColumnConfig)
	for _, proc := range runCfg.Procedures {
		tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
		cols, err := readColumnsFromCSV(tmplPath)
		if err != nil {
			log.Fatalf("Failed to read template for %s: %v", proc, err)
		}
		templates[proc] = cols
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

	procLogCh := make(chan ProcLog, 1000)
	summaryCh := make(chan SummaryUpdate, 1000)
	procSummary := make(map[string]ProcSummary)
	var summaryWg sync.WaitGroup

	summaryWg.Add(1)
	go func() {
		defer summaryWg.Done()
		for update := range summaryCh {
			s, exists := procSummary[update.Procedure]
			if !exists {
				s = ProcSummary{Procedure: update.Procedure, StartTime: update.StartTime, EndTime: update.EndTime, Status: update.Status}
			} else {
				if update.StartTime.Before(s.StartTime) {
					s.StartTime = update.StartTime
				}
				if update.EndTime.After(s.EndTime) {
					s.EndTime = update.EndTime
				}
				if s.Status != "FAIL" && update.Status == "FAIL" {
					s.Status = "FAIL"
				}
			}
			procSummary[update.Procedure] = s
		}
	}()

	if (mode == "I" && !runCfg.RunInsertionParallel) || (mode == "E" && !runCfg.RunExtractionParallel) {
		log.Println("Running procedures sequentially as parallel execution is disabled")
		appCfg.Concurrency = 1
	}

	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}

	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

	ctx := context.Background()
	overallStart := time.Now()

	for _, proc := range runCfg.Procedures {
		log.Printf("🚀 Starting procedure %s for all %d SOLs", proc, len(sols))
		procStart := time.Now()
		sem := make(chan struct{}, appCfg.Concurrency)
		var wg sync.WaitGroup

		for _, sol := range sols {
			wg.Add(1)
			sem <- struct{}{}
			go func(solID string) {
				defer wg.Done()
				defer func() { <-sem }()

				if mode == "E" {
					runExtraction(ctx, db, proc, solID, &runCfg, templates, procLogCh, summaryCh)
				} else if mode == "I" {
					runProcedure(ctx, db, proc, solID, &runCfg, procLogCh, summaryCh)
				}
			}(sol)
		}
		wg.Wait()
		log.Printf("🏁 Finished procedure %s in %s", proc, time.Since(procStart).Round(time.Second))
	}

	close(procLogCh)
	close(summaryCh)

	summaryWg.Wait()

	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
	if mode == "E" {
		mergeFiles(&runCfg)
	}
	log.Printf("🎯 All done! Processed %d SOLs in %s", len(sols), time.Since(overallStart).Round(time.Second))
}
