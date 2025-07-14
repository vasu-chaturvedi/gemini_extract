package main

import (
	"database/sql"
	"fmt"
	_ "github.com/godror/godror"
	"path/filepath"
	"time"
)

func setupDatabase(appCfg MainConfig) (*sql.DB, error) {
	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		appCfg.DBUser, appCfg.DBPassword, appCfg.DBHost, appCfg.DBPort, appCfg.DBSid)

	db, err := sql.Open("godror", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DB: %w", err)
	}

	db.SetMaxOpenConns(appCfg.Concurrency)
	db.SetMaxIdleConns(appCfg.Concurrency)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	return db, nil
}

func loadTemplates(runCfg ExtractionConfig, mode string) (map[string][]ColumnConfig, error) {
	templates := make(map[string][]ColumnConfig)
	if mode == "E" {
		for _, proc := range runCfg.Procedures {
			tmplPath := filepath.Join(runCfg.TemplatePath, fmt.Sprintf("%s.csv", proc))
			cols, err := readColumnsFromCSV(tmplPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read template for %s: %w", proc, err)
			}
			templates[proc] = cols
		}
	}
	return templates, nil
}

func determineLogFiles(runCfg ExtractionConfig, mode string) (string, string) {
	var LogFile, LogFileSummary string
	if mode == "I" {
		LogFile = runCfg.PackageName + "_insert.csv"
		LogFileSummary = runCfg.PackageName + "_insert_summary.csv"
	} else if mode == "E" {
		LogFile = runCfg.PackageName + "_extract.csv"
		LogFileSummary = runCfg.PackageName + "_extract_summary.csv"
	}
	return LogFile, LogFileSummary
}
