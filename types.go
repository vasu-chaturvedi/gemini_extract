package main

import (
	"time"
)

// Remove duplicate writeLog and writeSummary implementations from this file. Use writeLog.go for these functions.

type ProcLog struct {
	SolID         string
	Procedure     string
	StartTime     time.Time
	EndTime       time.Time
	ExecutionTime time.Duration
	Status        string
	ErrorDetails  string
}

type ProcSummary struct {
	Procedure  string
	StartTime  time.Time
	EndTime    time.Time
	Status     string
}

type ColumnConfig struct {
	Name     string
	Datatype string
	Format   string
	Length   int
	Align    string
}
