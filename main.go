package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"sync"

	tea "github.com/charmbracelet/bubbletea"
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

	templates, err := loadTemplates(runCfg, mode)
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

	if (mode == "I" && !runCfg.RunInsertionParallel) || (mode == "E" && !runCfg.RunExtractionParallel) {
		log.Println("Running procedures sequentially as parallel execution is disabled")
		appCfg.Concurrency = 1
	}

	LogFile, LogFileSummary := determineLogFiles(runCfg, mode)

	go writeLog(filepath.Join(appCfg.LogFilePath, LogFile), procLogCh)

	tasks := prepareTasks(sols, runCfg.Procedures)

	// Bubbletea program
	p := tea.NewProgram(NewTuiModel())

	// Start workers
	startWorkers(db, appCfg.Concurrency, appCfg.LogFilePath, LogFileSummary, runCfg, templates, procLogCh, &summaryMu, procSummary, tasks, p, mode)

	if err := p.Start(); err != nil {
		log.Fatalf("Alas, there's been an error: %v", err)
	}

	log.Println("🎯 All done!")
}