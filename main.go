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

type Job struct {
	SolID    string
	ProcName string
}

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

	ctx := context.Background()
	overallStart := time.Now()

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	if mode == "I" {
		// ----------------
		// NEW I-MODE LOGIC
		// ----------------
		jobs := make([]Job, 0, len(sols)*len(runCfg.Procedures))
		for _, sol := range sols {
			for _, proc := range runCfg.Procedures {
				jobs = append(jobs, Job{SolID: sol, ProcName: proc})
			}
		}
		totalJobs := len(jobs)
		jobCh := make(chan Job, totalJobs)

		for _, job := range jobs {
			jobCh <- job
		}
		close(jobCh)

		var wg sync.WaitGroup
		var completed int64 = 0

		for i := 0; i < appCfg.Concurrency; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for job := range jobCh {
					plog, _ := callProcedureJob(ctx, db, runCfg.PackageName, job.ProcName, job.SolID)
					procLogCh <- plog

					summaryMu.Lock()
					s, exists := procSummary[job.ProcName]
					if !exists {
						s = ProcSummary{Procedure: job.ProcName, StartTime: plog.StartTime, EndTime: plog.EndTime, Status: plog.Status}
					} else {
						if plog.StartTime.Before(s.StartTime) {
							s.StartTime = plog.StartTime
						}
						if plog.EndTime.After(s.EndTime) {
							s.EndTime = plog.EndTime
						}
						if s.Status != "FAIL" && plog.Status == "FAIL" {
							s.Status = "FAIL"
						}
					}
					procSummary[job.ProcName] = s
					summaryMu.Unlock()

					// Progress
					completed++
					if completed%500 == 0 || int(completed) == totalJobs {
						elapsed := time.Since(overallStart)
						estimatedTotal := time.Duration(float64(elapsed) / float64(completed) * float64(totalJobs))
						eta := estimatedTotal - elapsed
						log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
							completed, totalJobs, float64(completed)*100/float64(totalJobs),
							elapsed.Round(time.Second), eta.Round(time.Second))
					}
				}
			}(i)
		}
		wg.Wait()
		close(procLogCh)

		writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
		log.Printf("🎯 All done! Processed %d jobs in %s", totalJobs, time.Since(overallStart).Round(time.Second))

	} else if mode == "E" {
		// Extraction mode: unchanged!
		procCount := len(runCfg.Procedures)
		db.SetMaxOpenConns(appCfg.Concurrency * procCount)
		db.SetMaxIdleConns(appCfg.Concurrency * procCount)
		db.SetConnMaxLifetime(30 * time.Minute)

		if !runCfg.RunExtractionParallel {
			log.Println("Running procedures sequentially as parallel execution is disabled")
			appCfg.Concurrency = 1
		}

		sem := make(chan struct{}, appCfg.Concurrency)
		var wg sync.WaitGroup
		totalSols := len(sols)
		var mu sync.Mutex
		completed := 0

		for _, sol := range sols {
			wg.Add(1)
			sem <- struct{}{}
			go func(solID string) {
				defer wg.Done()
				defer func() { <-sem }()
				log.Printf("➡️ Starting SOL %s", solID)
				runExtractionForSol(ctx, db, solID, &runCfg, templates, procLogCh, &summaryMu, procSummary)
				mu.Lock()
				completed++
				if completed%100 == 0 || completed == totalSols {
					elapsed := time.Since(overallStart)
					estimatedTotal := time.Duration(float64(elapsed) / float64(completed) * float64(totalSols))
					eta := estimatedTotal - elapsed
					log.Printf("✅ Progress: %d/%d (%.2f%%) | Elapsed: %s | ETA: %s",
						completed, totalSols, float64(completed)*100/float64(totalSols),
						elapsed.Round(time.Second), eta.Round(time.Second))
				}
				mu.Unlock()
			}(sol)
		}
		wg.Wait()
		close(procLogCh)
		writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
		mergeFiles(&runCfg)
		log.Printf("🎯 All done! Processed %d SOLs in %s", totalSols, time.Since(overallStart).Round(time.Second))
	}
}
