package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Stats mirrors the structure from main.go
type Stats struct {
	TotalURLs       int    `json:"total_urls"`
	AnalyzedURLs    int    `json:"analyzed_urls"`
	UnanalyzedURLs  int    `json:"unanalyzed_urls"`
	CrawlerStatus   string `json:"crawler_status"`
	AnalyzerStatus  string `json:"analyzer_status"`
	BatchSize       int    `json:"batch_size"`
	LastUpdated     string `json:"last_updated"`
	Progress        int    `json:"progress"`
	TargetUrls      int    `json:"target_urls"`
	CrawlerPid      int    `json:"crawler_pid"`
	AnalyzerPid     int    `json:"analyzer_pid"`
}

func main() {
	// Command line flags
	statusFile := flag.String("status", "/var/ndaivimanuales/logs/ndaivi_status.json", "Path to status file")
	pidFile := flag.String("pid", "/var/ndaivimanuales/logs/ndaivi.pid", "Path to PID file")
	jsonOutput := flag.Bool("json", false, "Output in JSON format")
	verbose := flag.Bool("verbose", false, "Show verbose output")
	logLines := flag.Int("log", 10, "Number of recent log lines to show")
	flag.Parse()

	// Check if daemon is running
	pidRunning := false
	daemonPid := 0
	crawlerPid := 0
	analyzerPid := 0

	// Try to read PID file
	if pidData, err := os.ReadFile(*pidFile); err == nil {
		daemonPid, _ = strconv.Atoi(strings.TrimSpace(string(pidData)))
		if daemonPid > 0 {
			// Check if process is running
			cmd := exec.Command("ps", "-p", strconv.Itoa(daemonPid))
			if err := cmd.Run(); err == nil {
				pidRunning = true
			}
		}
	}

	// Read status file
	var stats Stats
	hasStats := false

	if data, err := os.ReadFile(*statusFile); err == nil {
		if json.Unmarshal(data, &stats) == nil {
			hasStats = true
			crawlerPid = stats.CrawlerPid
			analyzerPid = stats.AnalyzerPid
		}
	}

	// Last updated info
	updatedAgo := "unknown"
	if hasStats {
		if lastUpdated, err := time.Parse(time.RFC3339, stats.LastUpdated); err == nil {
			updatedAgo = fmt.Sprintf("%v ago", time.Since(lastUpdated).Round(time.Second))
		}
	}

	// Output in JSON format if requested
	if *jsonOutput {
		result := map[string]interface{}{
			"running":      pidRunning,
			"daemon_pid":   daemonPid,
			"crawler_pid":  crawlerPid,
			"analyzer_pid": analyzerPid,
			"updated_ago":  updatedAgo,
		}

		if hasStats {
			result["stats"] = stats
		}

		jsonData, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(jsonData))
		return
	}

	// Human readable output
	fmt.Println("NDAIVI Status")
	fmt.Println("=============")

	if pidRunning {
		fmt.Printf("Status: Running (PID: %d)\n", daemonPid)
	} else {
		fmt.Println("Status: Not running")
	}

	// Show stats if available
	if hasStats {
		fmt.Printf("Last Updated: %s (%s)\n", stats.LastUpdated, updatedAgo)
		fmt.Printf("Crawler: %s, Analyzer: %s\n", stats.CrawlerStatus, stats.AnalyzerStatus)
		progressPct := 0.0
		
		// Calculate progress percentage
		if stats.Progress > 0 {
			progressPct = float64(stats.Progress)
		} else if stats.TargetUrls > 0 && stats.TotalURLs > 0 {
			progressPct = float64(stats.TotalURLs) / float64(stats.TargetUrls) * 100.0
		} else if stats.TotalURLs > 0 {
			progressPct = float64(stats.AnalyzedURLs) / float64(stats.TotalURLs) * 100.0
		}
		
		fmt.Printf("Progress: %.1f%% (%d/%d URLs)\n", 
			progressPct, stats.TotalURLs, stats.TargetUrls)
		fmt.Printf("URLs: %d total, %d analyzed, %d unanalyzed\n", 
			stats.TotalURLs, stats.AnalyzedURLs, stats.UnanalyzedURLs)
	}

	// Show process IDs if verbose
	if *verbose {
		fmt.Println("\nProcess Information")
		fmt.Println("===================")
		if crawlerPid > 0 {
			fmt.Printf("Crawler PID: %d\n", crawlerPid)
		}
		if analyzerPid > 0 {
			fmt.Printf("Analyzer PID: %d\n", analyzerPid)
		}
		
		// Show process resources if running
		if pidRunning {
			cmd := exec.Command("ps", "-p", strconv.Itoa(daemonPid), "-o", "%cpu,%mem,vsz,rss,time")
			output, err := cmd.Output()
			if err == nil {
				lines := strings.Split(string(output), "\n")
				if len(lines) > 1 {
					fmt.Println("\nResource Usage:")
					fmt.Println(lines[0])
					fmt.Println(lines[1])
				}
			}
		}
		
		// Show recent logs if requested
		if *logLines > 0 {
			fmt.Println("\nRecent Logs:")
			cmd := exec.Command("tail", "-n", strconv.Itoa(*logLines), "/var/ndaivimanuales/logs/ndaivi.log")
			logOutput, err := cmd.Output()
			if err == nil {
				fmt.Println(string(logOutput))
			} else {
				fmt.Printf("Error reading logs: %v\n", err)
			}
		}
	}
}
