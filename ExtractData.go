package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func runExtractionForSol(ctx context.Context, db *sql.DB, solID string, procConfig *ExtractionConfig, templates map[string][]ColumnConfig, logCh chan<- ProcLog, mu *sync.Mutex, summary map[string]ProcSummary) {
	var wg sync.WaitGroup
	procCh := make(chan string)

	numWorkers := runtime.NumCPU() * 2
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for proc := range procCh {
				start := time.Now()
				log.Printf("📥 Extracting %s for SOL %s", proc, solID)
				err := extractData(ctx, db, proc, solID, procConfig, templates)
				end := time.Now()

				plog := ProcLog{
					SolID:         solID,
					Procedure:     proc,
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
				logCh <- plog

				mu.Lock()
				s, exists := summary[proc]
				if !exists {
					s = ProcSummary{Procedure: proc, StartTime: start, EndTime: end, Status: plog.Status}
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
				summary[proc] = s
				mu.Unlock()
				log.Printf("✅ Completed %s for SOL %s in %s", proc, solID, end.Sub(start).Round(time.Millisecond))
			}
		}()
	}

	for _, proc := range procConfig.Procedures {
		procCh <- proc
	}
	close(procCh)
	wg.Wait()
}

func extractData(ctx context.Context, db *sql.DB, procName, solID string, cfg *ExtractionConfig, templates map[string][]ColumnConfig) error {
	cols, ok := templates[procName]
	if !ok {
		return fmt.Errorf("missing template for procedure %s", procName)
	}

	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", strings.Join(colNames, ", "), procName)
	start := time.Now()
	rows, err := db.QueryContext(ctx, query, solID)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	log.Printf("🧮 Query executed for %s (SOL %s) in %s", procName, solID, time.Since(start).Round(time.Millisecond))

	spoolPath := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_%s.spool", procName, solID))
	f, err := os.Create(spoolPath)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewWriter(f)
	defer buf.Flush()

	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		var strValues []string
		for _, v := range values {
			if v.Valid {
				strValues = append(strValues, v.String)
			} else {
				strValues = append(strValues, "")
			}
		}
		buf.WriteString(formatRow(cfg, cols, strValues) + "\n")
	}
	return nil
}

func mergeFiles(cfg *ExtractionConfig) error {
	for _, proc := range cfg.Procedures {
		log.Printf("📦 Starting merge for procedure: %s", proc)

		pattern := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_*.spool", proc))
		finalFile := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s.txt", proc))

		files, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("glob failed: %w", err)
		}
		sort.Strings(files)

		outFile, err := os.Create(finalFile)
		if err != nil {
			return err
		}
		defer outFile.Close()

		writer := bufio.NewWriter(outFile)
		start := time.Now()

		for _, file := range files {
			in, err := os.Open(file)
			if err != nil {
				return err
			}
			scanner := bufio.NewScanner(in)
			for scanner.Scan() {
				writer.WriteString(scanner.Text() + "\n")
			}
			in.Close()
			os.Remove(file)
		}
		writer.Flush()
		log.Printf("📑 Merged %d files into %s in %s", len(files), finalFile, time.Since(start).Round(time.Second))
	}
	return nil
}

func readColumnsFromCSV(path string) ([]ColumnConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	csvr := csv.NewReader(r)
	headers, err := csvr.Read()
	if err != nil {
		return nil, err
	}
	index := make(map[string]int)
	for i, h := range headers {
		index[strings.ToLower(h)] = i
	}
	var cols []ColumnConfig
	for {
		row, err := csvr.Read()
		if err != nil {
			break
		}
		col := ColumnConfig{Name: row[index["name"]]}
		if i, ok := index["length"]; ok && i < len(row) {
			col.Length, _ = strconv.Atoi(row[i])
		}
		if i, ok := index["align"]; ok && i < len(row) {
			col.Align = row[i]
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func sanitize(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " ")
}

func formatRow(cfg *ExtractionConfig, cols []ColumnConfig, values []string) string {
	switch cfg.Format {
	case "delimited":
		var parts []string
		for _, v := range values {
			parts = append(parts, sanitize(v))
		}
		return strings.Join(parts, cfg.Delimiter)

	case "fixed":
		var out strings.Builder
		for i, col := range cols {
			var val string
			if i < len(values) && values[i] != "" {
				val = sanitize(values[i])
			} else {
				val = ""
			}

			if len(val) > col.Length {
				val = val[:col.Length]
			}

			if col.Align == "right" {
				out.WriteString(fmt.Sprintf("%*s", col.Length, val))
			} else {
				out.WriteString(fmt.Sprintf("%-*s", col.Length, val))
			}
		}
		return out.String()

	default:
		return ""
	}
}
