package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"sync"
)

var (
	AppCfgFile = new(string)
	RunCfgFile = new(string)
	Mode       string
)

func ParseAndValidateFlags() {
	flag.StringVar(AppCfgFile, "appCfg", "", "Path to the main application configuration file")
	flag.StringVar(RunCfgFile, "runCfg", "", "Path to the extraction configuration file")
	flag.StringVar(&Mode, "mode", "", "Mode of operation: E - Extract, I - Insert")
	flag.Parse()

	if Mode != "E" && Mode != "I" {
		log.Fatalf("Invalid mode. Valid values are 'E' for Extract and 'I' for Insert.")
	}
	if *AppCfgFile == "" || *RunCfgFile == "" {
		log.Fatalf("Both appCfg and runCfg must be specified")
	}
	if _, err := os.Stat(*AppCfgFile); os.IsNotExist(err) {
		log.Fatalf("Application configuration file does not exist: %s", *AppCfgFile)
	}
	if _, err := os.Stat(*RunCfgFile); os.IsNotExist(err) {
		log.Fatalf("Extraction configuration file does not exist: %s", *RunCfgFile)
	}
}

func main() {
	ParseAndValidateFlags()

	appCfg, err := loadMainConfig(*AppCfgFile)
	if err != nil {
		log.Fatalf("Failed to load main config: %v", err)
	}
	runCfg, err := loadExtractionConfig(*RunCfgFile)
	if err != nil {
		log.Fatalf("Failed to load extraction config: %v", err)
	}

	templates, err := loadTemplates(runCfg, Mode)
	if err != nil {
		log.Fatalf("Failed to load templates: %v", err)
	}

	db, err := setupDatabase(appCfg)
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}
	defer db.Close()

	sols, err := readSols(appCfg.SolFilePath)
	if err != nil {
		log.Fatalf("Failed to read SOL IDs: %v", err)
	}

	procLogCh := make(chan ProcLog, 1000)
	var summaryMu sync.Mutex
	procSummary := make(map[string]ProcSummary)

	if (Mode == "I" && !runCfg.RunInsertionParallel) || (Mode == "E" && !runCfg.RunExtractionParallel) {
		log.Println("Running procedures sequentially as parallel execution is disabled")
		appCfg.Concurrency = 1
	}

	LogFile, LogFileSummary := determineLogFiles(runCfg, Mode)

	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

	tasks := prepareTasks(sols, runCfg.Procedures)

	// Start workers
	startWorkers(db, appCfg.Concurrency, appCfg.LogFilePath, LogFileSummary, runCfg, templates, procLogCh, &summaryMu, procSummary, tasks, Mode)

	log.Println("🎯 All done!")
}