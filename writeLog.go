package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"

	log "github.com/charmbracelet/log"
)

// Write procedure logs to CSV file
func writeLog(path string, logCh <-chan ProcLog) {
	file, err := os.Create(path)
	if err != nil {
		log.Errorf("Failed to create procedure log file, logging will be disabled: %v", err)
		// Drain the channel to prevent the main application from blocking
		for range logCh {
		}
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"SOL_ID", "PROCEDURE", "START_TIME", "END_TIME", "EXECUTION_SECONDS", "STATUS", "ERROR_DETAILS"}); err != nil {
		log.Warnf("Failed to write header to procedure log: %v", err)
	}

	for plog := range logCh {
		errDetails := plog.ErrorDetails
		if errDetails == "" {
			errDetails = "-"
		}
		timeFormat := "02-01-2006 15:04:05"
		record := []string{
			plog.SolID,
			plog.Procedure,
			plog.StartTime.Format(timeFormat),
			plog.EndTime.Format(timeFormat),
			fmt.Sprintf("%.3f", plog.ExecutionTime.Seconds()),
			plog.Status,
			errDetails,
		}
		if err := writer.Write(record); err != nil {
			log.Warnf("Failed to write record to procedure log: %v", err)
		}
	}
}

// Write procedure summary CSV after all executions
func writeSummary(path string, summary map[string]ProcSummary) {
	file, err := os.Create(path)
	if err != nil {
		log.Errorf("Failed to create procedure summary file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	if err := writer.Write([]string{"PROCEDURE", "EARLIEST_START_TIME", "LATEST_END_TIME", "EXECUTION_SECONDS", "STATUS"}); err != nil {
		log.Warnf("Failed to write header to summary log: %v", err)
	}

	// Sort procedures alphabetically
	var procs []string
	for p := range summary {
		procs = append(procs, p)
	}
	sort.Strings(procs)

	for _, p := range procs {
		s := summary[p]
		execSeconds := s.EndTime.Sub(s.StartTime).Seconds()
		timeFormat := "02-01-2006 15:04:05"
		record := []string{
			p,
			s.StartTime.Format(timeFormat),
			s.EndTime.Format(timeFormat),
			fmt.Sprintf("%.3f", execSeconds),
			s.Status,
		}
		if err := writer.Write(record); err != nil {
			log.Warnf("Failed to write record to summary log: %v", err)
		}
	}
}
