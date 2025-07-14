package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
)

// Write procedure logs to CSV file
func writeLog(path string, logCh <-chan ProcLog) {
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Failed to create procedure log file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	writer.Write([]string{"SOL_ID", "PROCEDURE", "START_TIME", "END_TIME", "EXECUTION_SECONDS", "STATUS", "ERROR_DETAILS"})

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
		writer.Write(record)
	}
}

// Write procedure summary CSV after all executions
func writeSummary(path string, summary map[string]ProcSummary) {
	file, err := os.Create(path)
	if err != nil {
		log.Printf("Failed to create procedure summary file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Header
	writer.Write([]string{"PROCEDURE", "EARLIEST_START_TIME", "LATEST_END_TIME", "EXECUTION_SECONDS", "STATUS"})

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
		writer.Write([]string{
			p,
			s.StartTime.Format(timeFormat),
			s.EndTime.Format(timeFormat),
			fmt.Sprintf("%.3f", execSeconds),
			s.Status,
		})
	}
}
