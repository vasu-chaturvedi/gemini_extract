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

// workerFunc defines the signature for a function that performs the actual work on a job.
type workerFunc func(job Job) ProcLog

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

// runJobs handles the creation, distribution, and execution of jobs in a worker pool.
func runJobs(concurrency int, totalJobs int, jobCh <-chan Job, workerFn workerFunc, procLogCh chan<- ProcLog, procSummary map[string]ProcSummary, summaryMu *sync.Mutex) {
	var wg sync.WaitGroup
	var completed int64 = 0
	overallStart := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				plog := workerFn(job)
				procLogCh <- plog

				// Update summary
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

				// Progress Reporting
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
		}()
	}
	wg.Wait()
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

	// Load templates only if in extract mode
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

	procLogCh := make(chan ProcLog, 1000)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}

	logWriterDone := make(chan struct{})
	go func() {
		writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)
		close(logWriterDone)
	}()

	ctx := context.Background()
	overallStart := time.Now()

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	// Create the list of jobs
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

	// Define the specific worker task and run the jobs
	if mode == "I" {
		insertTask := func(job Job) ProcLog {
			plog, _ := callProcedureJob(ctx, db, runCfg.PackageName, job.ProcName, job.SolID)
			return plog
		}
		runJobs(appCfg.Concurrency, totalJobs, jobCh, insertTask, procLogCh, procSummary, &summaryMu)
	} else if mode == "E" {
		extractTask := func(job Job) ProcLog {
			start := time.Now()
			log.Printf("📥 Extracting %s for SOL %s", job.ProcName, job.SolID)
			err := extractData(ctx, db, job.ProcName, job.SolID, &runCfg, templates)
			end := time.Now()
			log.Printf("✅ Completed %s for SOL %s in %s", job.ProcName, job.SolID, end.Sub(start).Round(time.Millisecond))

			plog := ProcLog{
				SolID:         job.SolID,
				Procedure:     job.ProcName,
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
			return plog
		}
		runJobs(appCfg.Concurrency, totalJobs, jobCh, extractTask, procLogCh, procSummary, &summaryMu)
		mergeFiles(&runCfg)
	}

	close(procLogCh)
	<-logWriterDone // Wait for the log writer to finish
	writeSummary(filepath.Join(appCfg.LogFilePath, LogFileSummary), procSummary)
	log.Printf("🎯 All done! Processed %d jobs in %s", totalJobs, time.Since(overallStart).Round(time.Second))
}