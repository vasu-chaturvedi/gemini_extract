package main

import (
	"bytes"
	"flag"
	"log"
	"os"
	"testing"
)

// Helper function to capture log.Fatalf output
func captureFatal(t *testing.T, f func(), expectedLogSubstring string) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr) // Reset output
	}()

	// Use a channel to signal when the goroutine is done
	done := make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Recover from log.Fatal, which calls os.Exit
				if r != "os.Exit(1)" { // Check if it's our expected exit
					t.Errorf("Unexpected panic: %v", r)
				}
			}
			close(done)
		}()
		f()
	}()

	<-done // Wait for the goroutine to finish

	actualLog := buf.String()
	if actualLog == "" {
		t.Errorf("Expected log output, but got none")
	} else if !bytes.Contains(buf.Bytes(), []byte(expectedLogSubstring)) {
		t.Errorf("Expected log to contain: %q, Got: %q", expectedLogSubstring, actualLog)
	}
}

func TestParseAndValidateFlags(t *testing.T) {
	// Save original os.Args
	oldArgs := os.Args

	// Create dummy config files for testing
	dummyAppCfg := "test_app_cfg.json"
	dummyRunCfg := "test_run_cfg.json"
	os.WriteFile(dummyAppCfg, []byte("{}"), 0644)
	os.WriteFile(dummyRunCfg, []byte("{}"), 0644)
	defer os.Remove(dummyAppCfg)
	defer os.Remove(dummyRunCfg)

	tests := []struct {
		name        string
		args        []string
		expectedLog string
		shouldPanic bool
	}{
		{
			name:        "Missing appCfg",
			args:        []string{"cmd", "-runCfg", dummyRunCfg, "-mode", "E"},
			expectedLog: "Both appCfg and runCfg must be specified",
			shouldPanic: true,
		},
		{
			name:        "Missing runCfg",
			args:        []string{"cmd", "-appCfg", dummyAppCfg, "-mode", "E"},
			expectedLog: "Both appCfg and runCfg must be specified",
			shouldPanic: true,
		},
		{
			name:        "Invalid mode",
			args:        []string{"cmd", "-appCfg", dummyAppCfg, "-runCfg", dummyRunCfg, "-mode", "X"},
			expectedLog: "Invalid mode. Valid values are 'E' for Extract and 'I' for Insert.",
			shouldPanic: true,
		},
		{
			name:        "Non-existent appCfg",
			args:        []string{"cmd", "-appCfg", "non_existent_app.json", "-runCfg", dummyRunCfg, "-mode", "E"},
			expectedLog: "Application configuration file does not exist: non_existent_app.json",
			shouldPanic: true,
		},
		{
			name:        "Non-existent runCfg",
			args:        []string{"cmd", "-appCfg", dummyAppCfg, "-runCfg", "non_existent_run.json", "-mode", "E"},
			expectedLog: "Extraction configuration file does not exist: non_existent_run.json",
			shouldPanic: true,
		},
		{
			name:        "Valid arguments - Extract mode",
			args:        []string{"cmd", "-appCfg", dummyAppCfg, "-runCfg", dummyRunCfg, "-mode", "E"},
			expectedLog: "", // No log.Fatal expected
			shouldPanic: false,
		},
		{
			name:        "Valid arguments - Insert mode",
			args:        []string{"cmd", "-appCfg", dummyAppCfg, "-runCfg", dummyRunCfg, "-mode", "I"},
			expectedLog: "", // No log.Fatal expected
			shouldPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = tt.args
			// Reset flags for each test run
			flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
			AppCfgFile = new(string)
			RunCfgFile = new(string)
			Mode = ""

			if tt.shouldPanic {
				captureFatal(t, func() {
					ParseAndValidateFlags() // Call the new function
				}, tt.expectedLog)
			} else {
				// For valid cases, we expect no panic and no log.Fatal output
				var buf bytes.Buffer
				log.SetOutput(&buf)
				defer log.SetOutput(os.Stderr)
				ParseAndValidateFlags()
				if buf.String() != "" {
					t.Errorf("Unexpected log output for valid case: %q", buf.String())
				}
			}
		})
	}

	// Restore original os.Args
	os.Args = oldArgs
}
