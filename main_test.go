package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	_ "github.com/godror/godror"
)

// BenchmarkExtractData benchmarks the extractData function.
// It requires a running database with the specified schema and data.
// To run this benchmark, use the following command:
// go test -bench=. -benchmem -run=^#
func BenchmarkExtractData(b *testing.B) {
	// --- Test Setup ---
	// IMPORTANT: Replace with your actual database connection details for benchmarking
	// You can use environment variables to make this more flexible.
	cfg := MainConfig{
		DBUser:     "user",
		DBPassword: "password",
		DBHost:     "localhost",
		DBPort:     1521,
		DBSid:      "orcl",
	}
	extractCfg := ExtractionConfig{
		SpoolOutputPath: "./spool",
		Format:          "delimited",
		Delimiter:       "|",
	}

	// Create spool directory if it doesn't exist
	if err := os.MkdirAll(extractCfg.SpoolOutputPath, 0755); err != nil {
		b.Fatalf("Failed to create spool directory: %v", err)
	}

	connString := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s"`,
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBSid)

	db, err := sql.Open("godror", connString)
	if err != nil {
		b.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	// --- Prepare Statement ---
	// IMPORTANT: Replace with a real procedure and SOL ID for your benchmark
	procName := "YOUR_PROCEDURE_NAME"
	solID := "YOUR_SOL_ID"
	templates := map[string][]ColumnConfig{
		procName: {
			{Name: "COLUMN1"},
			{Name: "COLUMN2"},
			// Add all columns for your procedure
		},
	}

	cols := templates[procName]
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE SOL_ID = :1", colNames, procName)
	stmt, err := db.Prepare(query)
	if err != nil {
		b.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// --- Create Sync Pool ---
	slicePool := &sync.Pool{
		New: func() interface{} {
			return make([]interface{}, len(cols))
		},
	}

	// --- Run Benchmark ---
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := extractData(context.Background(), stmt, slicePool, procName, solID, &extractCfg, templates)
		if err != nil {
			b.Fatalf("extractData failed: %v", err)
		}
	}
}
