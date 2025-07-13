package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

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
