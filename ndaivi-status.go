package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type Stats struct {
	TotalURLs      int    `json:"total_urls"`
	AnalyzedURLs    int    `json:"analyzed_urls"`
	UnanalyzedURLs  int    `json:"unanalyzed_urls"`
	CrawlerStatus   string `json:"crawler_status"`
	AnalyzerStatus  string `json:"analyzer_status"`
	BatchSize       int    `json:"batch_size"`
	LastUpdated     string `json:"last_updated"`
}

func main() {
	statusFile := flag.String("status", "/var/ndaivimanuales/logs/ndaivi_status.json", "Path to status file")
	pidFile := flag.String("pid", "/var/run/ndaivi.pid", "Path to PID file")
	jsonOutput := flag.Bool("json", false, "Output in JSON format")
	verbose := flag.Bool("verbose", false, "Show verbose output")
	flag.Parse()

	// Check if status file exists
	stats, err := readStats(*statusFile)
	if err != nil {
		fmt.Printf("Error reading status: %v\n", err)
		os.Exit(1)
	}

	// Check if NDAIVI process is running
	pidRunning := false
	var pid int
	if pidBytes, err := ioutil.ReadFile(*pidFile); err == nil {
		pid, err = strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		if err == nil {
			// Check if process exists
			cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "cmd=")
			output, err := cmd.Output()
			if err == nil && strings.Contains(string(output), "ndaivi") {
				pidRunning = true
			}
		}
	}

	// Format the last updated time
	lastUpdated, _ := time.Parse(time.RFC3339, stats.LastUpdated)
	updatedAgo := time.Since(lastUpdated).Round(time.Second)

	// Output in JSON format if requested
	if *jsonOutput {
		result := map[string]interface{}{
			"stats": stats,
			"pid": pid,
			"pid_running": pidRunning,
			"updated_ago": updatedAgo.String(),
		}
		jsonData, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(jsonData))
		return
	}

	// Output in human-readable format
	fmt.Printf("NDAIVI Status\n")
	fmt.Printf("=============\n")
	
	if pidRunning {
		fmt.Printf("Status: Running (PID: %d)\n", pid)
	} else if pid > 0 {
		fmt.Printf("Status: Not Running (Last PID: %d)\n", pid)
	} else {
		fmt.Printf("Status: Not Running\n")
	}
	
	fmt.Printf("Last Updated: %s (%s ago)\n", stats.LastUpdated, updatedAgo)
	fmt.Printf("Crawler: %s, Analyzer: %s\n", stats.CrawlerStatus, stats.AnalyzerStatus)
	fmt.Printf("URLs: %d total, %d analyzed, %d unanalyzed\n", 
		stats.TotalURLs, stats.AnalyzedURLs, stats.UnanalyzedURLs)
	fmt.Printf("Analyzer Batch: %d URLs\n", stats.BatchSize)
	
	if *verbose {
		// Add more detailed information in verbose mode
		fmt.Printf("\nDetailed Stats\n")
		fmt.Printf("=============\n")
		fmt.Printf("Progress: %.1f%%\n", getProgress(stats))
		
		// Print crawler performance
		if pidRunning {
			// Get process CPU and memory usage
			cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "%cpu,%mem")
			output, err := cmd.Output()
			if err == nil {
				lines := strings.Split(string(output), "\n")
				if len(lines) >= 2 {
					fmt.Printf("Resource Usage: %s\n", strings.TrimSpace(lines[1]))
				}
			}
		}
	}
}

func readStats(path string) (Stats, error) {
	var stats Stats

	// Read the status file
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return stats, err
	}

	// Parse JSON
	err = json.Unmarshal(data, &stats)
	if err != nil {
		return stats, err
	}

	return stats, nil
}

func getProgress(stats Stats) float64 {
	if stats.TotalURLs == 0 {
		return 0.0
	}
	return float64(stats.AnalyzedURLs) / float64(stats.TotalURLs) * 100.0
}
