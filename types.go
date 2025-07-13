package main

import "time"

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
	Procedure string
	StartTime time.Time
	EndTime   time.Time
	Status    string
}

type SummaryUpdate struct {
	Procedure string
	StartTime time.Time
	EndTime   time.Time
	Status    string
}

type ColumnConfig struct {
	Name   string `json:"name"`
	Length int    `json:"length"`
	Align  string `json:"align"`
}