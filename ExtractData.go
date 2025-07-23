package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/charmbracelet/log"
)

// extractData performs the data extraction for a single procedure and SOL ID.
// It uses a prepared statement for querying and a sync.Pool for slice reuse to optimize performance.
func extractData(ctx context.Context, stmt *sql.Stmt, slicePool *sync.Pool, procName, solID string, cfg *ExtractionConfig, templates map[string][]ColumnConfig) error {
	cols, ok := templates[procName]
	if !ok {
		return fmt.Errorf("missing template for procedure %s", procName)
	}

	start := time.Now()
	rows, err := stmt.QueryContext(ctx, solID)
	if err != nil {
		return fmt.Errorf("prepared statement query failed for procedure %s: %w", procName, err)
	}
	defer rows.Close()
	log.Debug("Query executed", "procedure", procName, "sol_id", solID, "duration", time.Since(start).Round(time.Millisecond))

	spoolPath := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_%s.spool", procName, solID))
	f, err := os.Create(spoolPath)
	if err != nil {
		return fmt.Errorf("failed to create spool file %s: %w", spoolPath, err)
	}
	defer f.Close()

	buf := bufio.NewWriter(f)
	defer buf.Flush()

	// Setup writer based on format
	var csvWriter *csv.Writer
	if cfg.Format == "delimited" {
		csvWriter = csv.NewWriter(buf)
		if len(cfg.Delimiter) == 1 {
			csvWriter.Comma = []rune(cfg.Delimiter)[0]
		} else {
			log.Warn("Delimiter is not a single character, using default comma", "delimiter", cfg.Delimiter)
			// Default is comma, so no action needed
		}
		defer csvWriter.Flush()
	}

	// Get a slice from the pool for scanning
	scanArgs := slicePool.Get().([]interface{})
	defer slicePool.Put(scanArgs) // Return the slice to the pool when done

	// Ensure the slice is the correct size for the number of columns
	if len(scanArgs) < len(cols) {
		scanArgs = make([]interface{}, len(cols))
	}

	values := make([]sql.NullString, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs[:len(cols)]...); err != nil {
			return fmt.Errorf("failed to scan row for procedure %s: %w", procName, err)
		}

		var strValues []string
		for _, v := range values {
			if v.Valid {
				strValues = append(strValues, sanitize(v.String))
			} else {
				strValues = append(strValues, "")
			}
		}

		switch cfg.Format {
		case "delimited":
			if err := csvWriter.Write(strValues); err != nil {
				return fmt.Errorf("failed to write csv row for procedure %s: %w", procName, err)
			}
		case "fixed":
			var out strings.Builder
			for i, col := range cols {
				var val string
				if i < len(strValues) {
					val = strValues[i]
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
			if _, err := buf.WriteString(out.String() + "\n"); err != nil {
				return fmt.Errorf("failed to write fixed-width row for procedure %s: %w", procName, err)
			}
		}
	}
	if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating rows for procedure %s: %w", procName, err)
	}
	return nil
}


func mergeFiles(cfg *ExtractionConfig) error {
	for _, proc := range cfg.Procedures {
		log.Info("📦 Starting merge", "procedure", proc)

		pattern := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s_*.spool", proc))
		finalFile := filepath.Join(cfg.SpoolOutputPath, fmt.Sprintf("%s.txt", proc))

		files, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("glob failed for pattern %s: %w", pattern, err)
		}
		if len(files) == 0 {
			log.Warn("No spool files found to merge", "procedure", proc, "pattern", pattern)
			continue
		}
		sort.Strings(files)

		outFile, err := os.Create(finalFile)
		if err != nil {
			return fmt.Errorf("failed to create final output file %s: %w", finalFile, err)
		}
		defer outFile.Close()

		writer := bufio.NewWriter(outFile)
		start := time.Now()

		var mergedCount int
		for _, file := range files {
			in, err := os.Open(file)
			if err != nil {
				log.Error("Failed to open spool file for merging, skipping", "file", file, "error", err)
				continue
			}

			scanner := bufio.NewScanner(in)
			for scanner.Scan() {
				if _, err := writer.WriteString(scanner.Text() + "\n"); err != nil {
					in.Close() // Close before returning
					return fmt.Errorf("failed to write to merged file %s: %w", finalFile, err)
				}
			}
			in.Close()
			if err := os.Remove(file); err != nil {
				log.Warn("Failed to remove spool file", "file", file, "error", err)
			}
			mergedCount++
		}
		writer.Flush()
		log.Info("📑 Merged files", "count", mergedCount, "output_file", finalFile, "duration", time.Since(start).Round(time.Second))
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
		return nil, fmt.Errorf("failed to read csv header from %s: %w", path, err)
	}
	index := make(map[string]int)
	for i, h := range headers {
		index[strings.ToLower(h)] = i
	}

	nameIndex, ok := index["name"]
	if !ok {
		return nil, fmt.Errorf("missing required 'name' column in csv template: %s", path)
	}

	var cols []ColumnConfig
	for {
		row, err := csvr.Read()
		if err != nil {
			break // End of file
		}
		col := ColumnConfig{Name: row[nameIndex]}
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
