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
	"time"
)

// extractData now accepts a prepared statement for efficiency.
func extractData(ctx context.Context, stmt *sql.Stmt, procName, solID string, cfg *ExtractionConfig, cols []ColumnConfig) error {
	rows, err := stmt.QueryContext(ctx, solID)
	if err != nil {
		return fmt.Errorf("prepared statement query failed for procedure %s, SOL %s: %w", procName, solID, err)
	}
	defer rows.Close()

	spoolPath := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_%s.spool", procName, solID))
	f, err := os.Create(spoolPath)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewWriter(f)
	defer buf.Flush()

	// --- Memory Allocation Improvement ---
	// Allocate slices once before the loop to reduce garbage collection.
	values := make([]sql.NullString, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		// Reuse the same slices for every row.
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
				log.Printf("Error opening spool file for merge, skipping: %s, %v", file, err)
				continue
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

// formatRow no longer calls the sanitize function.
func formatRow(cfg *ExtractionConfig, cols []ColumnConfig, values []string) string {
	switch cfg.Format {
	case "delimited":
		// The call to sanitize() has been removed here.
		return strings.Join(values, cfg.Delimiter)

	case "fixed":
		var out strings.Builder
		for i, col := range cols {
			var val string
			if i < len(values) && values[i] != "" {
				// The call to sanitize() has been removed here.
				val = values[i]
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