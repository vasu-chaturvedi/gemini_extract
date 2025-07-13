package main

import (
	"bufio"
	"encoding/json"
	"os"
)

type MainConfig struct {
	DBUser      string `json:"db_user"`
	DBPassword  string `json:"db_password"`
	DBHost      string `json:"db_host"`
	DBPort      int    `json:"db_port"`
	DBSid       string `json:"db_sid"`
	Concurrency int    `json:"concurrency"`
	LogFilePath string `json:"log_path"`
	SolFilePath string `json:"sol_list_path"`
}

type ExtractionConfig struct {
	PackageName           string   `json:"package_name"`
	Procedures            []string `json:"procedures"`
	SpoolOutputPath       string   `json:"spool_output_path"`
	RunInsertionParallel  bool     `json:"run_insertion_parallel"`
	RunExtractionParallel bool     `json:"run_extraction_parallel"`
	TemplatePath          string   `json:"template_path"`
	Format                string   `json:"format"`
	Delimiter             string   `json:"delimiter"`
}

func loadMainConfig(path string) (MainConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return MainConfig{}, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var cfg MainConfig
	err = decoder.Decode(&cfg)
	return cfg, err
}

func loadExtractionConfig(path string) (ExtractionConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return ExtractionConfig{}, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var cfg ExtractionConfig
	err = decoder.Decode(&cfg)
	return cfg, err
}

func readSols(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var sols []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			sols = append(sols, line)
		}
	}
	return sols, scanner.Err()
}
