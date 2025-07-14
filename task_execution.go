package main

import (
	"context"
	"database/sql"
	
	"path/filepath"
	"sync"
	"time"
)

func prepareTasks(sols []string, procedures []string) []ProcTask {
	tasks := make([]ProcTask, 0, len(sols)*len(procedures))
	for _, sol := range sols {
		for _, proc := range procedures {
			tasks = append(tasks, ProcTask{SolID: sol, Procedure: proc})
		}
	}
	return tasks
}

func startWorkers(db *sql.DB, concurrency int, logFilePath, logFileSummary string, runCfg ExtractionConfig, templates map[string][]ColumnConfig, procLogCh chan<- ProcLog, summaryMu *sync.Mutex, procSummary map[string]ProcSummary, tasks []ProcTask, mode string) {
	taskCh := make(chan ProcTask, len(tasks))
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)

	go func() {
		var wg sync.WaitGroup
		ctx := context.Background()
		completedTasks := 0

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range taskCh {
					start := time.Now()
					var err error

					if mode == "E" {
						err = extractData(ctx, db, task.Procedure, task.SolID, &runCfg, templates)
					} else {
						err = callProcedure(ctx, db, runCfg.PackageName, task.Procedure, task.SolID)
					}

					end := time.Now()
					plog := ProcLog{
						SolID:         task.SolID,
						Procedure:     task.Procedure,
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
					procLogCh <- plog

					summaryMu.Lock()
					s, exists := procSummary[task.Procedure]
					if !exists {
						s = ProcSummary{Procedure: task.Procedure, StartTime: start, EndTime: end, Status: plog.Status}
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
					procSummary[task.Procedure] = s
					summaryMu.Unlock()

					completedTasks++
					// p.Send(progressMsg(float64(completedTasks) / float64(totalTasks)))
				}
			}()
		}

		wg.Wait()
		close(procLogCh)
		writeSummary(filepath.Join(logFilePath, logFileSummary), procSummary)
		if mode == "E" {
			mergeFiles(&runCfg)
		}
		// p.Quit()
	}()
}
