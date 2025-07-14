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
	
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)



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
	var wg sync.WaitGroup
	errCh := make(chan error, len(cfg.Procedures))

	for _, proc := range cfg.Procedures {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			log.Printf("📦 Starting merge for procedure: %s", p)

			pattern := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_*.spool", p))
			finalFile := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s.txt", p))

			files, err := filepath.Glob(pattern)
			if err != nil {
				errCh <- fmt.Errorf("glob failed for %s: %w", p, err)
				return
			}
			sort.Strings(files)

			outFile, err := os.Create(finalFile)
			if err != nil {
				errCh <- err
				return
			}
			defer outFile.Close()

			writer := bufio.NewWriter(outFile)
			start := time.Now()

			for _, file := range files {
				in, err := os.Open(file)
				if err != nil {
					errCh <- err
					return
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
		}(proc)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		if err != nil {
			return err // Return the first error encountered
		}
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
