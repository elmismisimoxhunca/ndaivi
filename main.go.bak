package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v3"
)

type Config struct {
	PostgresURL    string
	SQLiteDBPath   string
	MaxBatchSize   int
	MaxUnanalyzedURLs int
	SpeedCheckInterval int
	TargetWebsite  string
	MaxUrls        int
	CrawlerScript  string
	AnalyzerScript string
	StatusFile     string
	LogFile        string
	PidFile        string
}

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

type NDAIVI struct {
	config         Config
	pgDB          *sql.DB
	sqliteDB      *sql.DB
	crawlerCmd    *exec.Cmd
	analyzerCmd   *exec.Cmd
	analyzerBatch []string
	stats         Stats
	stopChan      chan struct{}
	wg            sync.WaitGroup
	mu            sync.Mutex
}

func NewNDAIVI(config Config) (*NDAIVI, error) {
	// Connect to PostgreSQL database
	pgDB, err := sql.Open("postgres", config.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	
	// Verify PostgreSQL connection
	if err := pgDB.Ping(); err != nil {
		log.Printf("Warning: PostgreSQL connection not verified: %v", err)
	}

	// Check if SQLite database file exists
	if _, err := os.Stat(config.SQLiteDBPath); os.IsNotExist(err) {
		log.Printf("Warning: SQLite database file %s does not exist. Waiting for Python crawler to initialize it", config.SQLiteDBPath)
	}

	// Connect to SQLite database (don't initialize, that's the Python crawler's job)
	sqliteDB, err := sql.Open("sqlite3", config.SQLiteDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite: %w", err)
	}

	return &NDAIVI{
		config:         config,
		pgDB:          pgDB,
		sqliteDB:      sqliteDB,
		analyzerBatch: make([]string, 0, config.MaxBatchSize),
		stopChan:      make(chan struct{}),
	}, nil
}

func (n *NDAIVI) Start() error {
	log.Println("Starting NDAIVI system...")

	// Start crawler
	if err := n.startCrawler(); err != nil {
		return fmt.Errorf("failed to start crawler: %w", err)
	}

	// Start analyzer
	if err := n.startAnalyzer(); err != nil {
		return fmt.Errorf("failed to start analyzer: %w", err)
	}

	// Start monitoring routines
	n.wg.Add(3)
	go n.monitorCrawlerSpeed()
	go n.monitorStats()
	go n.processAnalyzerOutput()

	return nil
}

func (n *NDAIVI) Stop() {
	log.Println("Stopping NDAIVI system...")
	close(n.stopChan)
	
	// Stop components
	if n.crawlerCmd != nil && n.crawlerCmd.Process != nil {
		n.crawlerCmd.Process.Signal(syscall.SIGTERM)
	}
	if n.analyzerCmd != nil && n.analyzerCmd.Process != nil {
		n.analyzerCmd.Process.Signal(syscall.SIGTERM)
	}

	// Wait for all goroutines to finish
	n.wg.Wait()

	// Close database connections
	if n.pgDB != nil {
		n.pgDB.Close()
	}
	if n.sqliteDB != nil {
		n.sqliteDB.Close()
	}

	// Remove PID file if it exists and belongs to us
	if n.config.PidFile != "" {
		if data, err := os.ReadFile(n.config.PidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid == os.Getpid() {
				os.Remove(n.config.PidFile)
			}
		}
	}

	log.Println("NDAIVI system stopped")
}

func (n *NDAIVI) startCrawler() error {
	// Pass target website and other config via environment variables
	env := os.Environ()
	env = append(env, fmt.Sprintf("NDAIVI_TARGET_WEBSITE=%s", n.config.TargetWebsite))
	env = append(env, fmt.Sprintf("NDAIVI_MAX_URLS=%d", n.config.MaxUrls))
	env = append(env, fmt.Sprintf("NDAIVI_DB_PATH=%s", n.config.SQLiteDBPath))
	env = append(env, fmt.Sprintf("NDAIVI_LOG_FILE=%s", n.config.LogFile))
	
	// Create log directory if it doesn't exist
	logDir := filepath.Dir(n.config.LogFile)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("Warning: Failed to create log directory: %v", err)
	}
	
	n.crawlerCmd = exec.Command("python3", n.config.CrawlerScript)
	n.crawlerCmd.Env = env
	n.crawlerCmd.Stdout = os.Stdout
	n.crawlerCmd.Stderr = os.Stderr
	
	// Log the command we're about to run
	log.Printf("Starting crawler: %s %s", "python3", n.config.CrawlerScript)
	log.Printf("Environment: NDAIVI_TARGET_WEBSITE=%s, NDAIVI_MAX_URLS=%d", 
		n.config.TargetWebsite, n.config.MaxUrls)
	
	if err := n.crawlerCmd.Start(); err != nil {
		return err
	}
	
	// Store the crawler PID for status reporting
	if n.crawlerCmd.Process != nil {
		n.stats.CrawlerPid = n.crawlerCmd.Process.Pid
		n.stats.CrawlerStatus = "running"
		n.updateStats() // Update status file immediately
	}

	// Monitor crawler process
	go func() {
		if err := n.crawlerCmd.Wait(); err != nil {
			log.Printf("Crawler process exited with error: %v", err)
			// Update status
			n.stats.CrawlerStatus = "stopped"
			n.updateStats()
			// Attempt to restart crawler after brief delay
			time.Sleep(5 * time.Second)
			if err := n.startCrawler(); err != nil {
				log.Printf("Failed to restart crawler: %v", err)
			}
		} else {
			log.Printf("Crawler process exited normally")
			n.stats.CrawlerStatus = "stopped"
			n.updateStats()
		}
	}()

	return nil
}

func (n *NDAIVI) startAnalyzer() error {
	n.analyzerCmd = exec.Command("python3", n.config.AnalyzerScript)
	n.analyzerCmd.Stdout = os.Stdout
	n.analyzerCmd.Stderr = os.Stderr

	if err := n.analyzerCmd.Start(); err != nil {
		return err
	}

	return nil
}

func (n *NDAIVI) monitorCrawlerSpeed() {
	defer n.wg.Done()
	ticker := time.NewTicker(time.Duration(n.config.SpeedCheckInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			unanalyzed, err := n.getUnanalyzedURLCount()
			if err != nil {
				log.Printf("Error getting unanalyzed URL count: %v", err)
				continue
			}

			threshold := n.config.MaxUnanalyzedURLs
			if unanalyzed > threshold {
				log.Printf("Unanalyzed URLs (%d) exceeds threshold (%d), slowing down crawler", unanalyzed, threshold)
				n.setCrawlerSpeed("slow")
			} else if unanalyzed < threshold*3/4 { // 75% of threshold
				log.Printf("Unanalyzed URLs (%d) below threshold (%d), resuming normal crawler speed", unanalyzed, threshold*3/4)
				n.setCrawlerSpeed("normal")
			}
		}
	}
}

func (n *NDAIVI) monitorStats() {
	defer n.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			n.updateStats()
		}
	}
}

func (n *NDAIVI) processAnalyzerOutput() {
	defer n.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			n.processBatch()
		}
	}
}

func (n *NDAIVI) setCrawlerSpeed(speed string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.crawlerCmd == nil || n.crawlerCmd.Process == nil {
		return
	}

	delay := "0"
	if speed == "slow" {
		delay = "2"
	}

	cmd := fmt.Sprintf("SPEED:%s\n", delay)
	if stdin, err := n.crawlerCmd.StdinPipe(); err == nil {
		stdin.Write([]byte(cmd))
	}
}

func (n *NDAIVI) getUnanalyzedURLCount() (int, error) {
	var count int
	
	// Check if the table exists before querying
	var tableName string
	err := n.sqliteDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'").Scan(&tableName)
	if err != nil {
		if err == sql.ErrNoRows {
			// Table doesn't exist yet - Python hasn't initialized the DB
			return 0, nil
		}
		return 0, err
	}
	
	// Table exists, query for unanalyzed count
	err = n.sqliteDB.QueryRow("SELECT COUNT(*) FROM pages WHERE analyzed = 0").Scan(&count)
	if err != nil {
		// Handle case where 'analyzed' column might not exist
		if strings.Contains(err.Error(), "no such column") {
			log.Printf("Warning: 'analyzed' column not found in pages table")
			return 0, nil
		}
		return 0, err
	}
	
	return count, nil
}

func (n *NDAIVI) updateStats() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Update timestamp
	n.stats.LastUpdated = time.Now().Format(time.RFC3339)

	// Get URL counts from SQLite database
	if n.sqliteDB != nil {
		// Check if the table exists before querying
		var tableName string
		err := n.sqliteDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'").Scan(&tableName)
		if err != nil {
			if err == sql.ErrNoRows {
				// Table doesn't exist yet
				n.stats.TotalURLs = 0
				n.stats.AnalyzedURLs = 0
				n.stats.UnanalyzedURLs = 0
			} else {
				log.Printf("Error checking for pages table: %v", err)
			}
		} else {
			// Table exists, get URL counts
			n.sqliteDB.QueryRow("SELECT COUNT(*) FROM pages").Scan(&n.stats.TotalURLs)
			
			// Check if analyzed column exists
			rows, err := n.sqliteDB.Query("PRAGMA table_info(pages)")
			if err != nil {
				log.Printf("Error querying table schema: %v", err)
			} else {
				hasAnalyzedColumn := false
				defer rows.Close()
				
				for rows.Next() {
					var cid, notnull, pk int
					var name, ctype, dfltValue string
					if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
						log.Printf("Error scanning column info: %v", err)
						continue
					}
					if name == "analyzed" {
						hasAnalyzedColumn = true
						break
					}
				}
				
				if hasAnalyzedColumn {
					n.sqliteDB.QueryRow("SELECT COUNT(*) FROM pages WHERE analyzed = 1").Scan(&n.stats.AnalyzedURLs)
					n.sqliteDB.QueryRow("SELECT COUNT(*) FROM pages WHERE status_code = 200 AND analyzed = 0").Scan(&n.stats.UnanalyzedURLs)
				} else {
					// No analyzed column, assume all unanalyzed
					n.stats.AnalyzedURLs = 0
					n.stats.UnanalyzedURLs = n.stats.TotalURLs
				}
			}
		}
		
		// Update progress percentage
		n.stats.TargetUrls = n.config.MaxUrls
		if n.stats.TargetUrls > 0 && n.stats.TotalURLs > 0 {
			progressPercent := (n.stats.TotalURLs * 100) / n.stats.TargetUrls
			if progressPercent > 100 {
				progressPercent = 100
			}
			n.stats.Progress = progressPercent
		} else {
			n.stats.Progress = 0
		}
	} else {
		log.Printf("Warning: SQLite database connection is nil")
	}

	// Update process status and PIDs
	if n.crawlerCmd != nil && n.crawlerCmd.Process != nil {
		// Check if process exists
		if err := n.crawlerCmd.Process.Signal(syscall.Signal(0)); err != nil {
			log.Printf("Crawler process signal check failed: %v", err)
			n.stats.CrawlerStatus = "stopped"
		} else {
			n.stats.CrawlerStatus = "running"
		}
		n.stats.CrawlerPid = n.crawlerCmd.Process.Pid
	} else {
		n.stats.CrawlerStatus = "stopped"
		n.stats.CrawlerPid = 0
	}

	if n.analyzerCmd != nil && n.analyzerCmd.Process != nil {
		// Check if process exists
		if err := n.analyzerCmd.Process.Signal(syscall.Signal(0)); err != nil {
			n.stats.AnalyzerStatus = "stopped"
		} else {
			n.stats.AnalyzerStatus = "running"
		}
		n.stats.AnalyzerPid = n.analyzerCmd.Process.Pid
	} else {
		n.stats.AnalyzerStatus = "stopped"
		n.stats.AnalyzerPid = 0
	}

	n.stats.BatchSize = len(n.analyzerBatch)

	// Write stats to file
	statsJSON, _ := json.MarshalIndent(n.stats, "", "  ")
	os.WriteFile(n.config.StatusFile, statsJSON, 0644)
} else {
		log.Printf("Warning: SQLite database connection is nil")
	}
	
	// Update crawler status if not running
	if n.crawlerCmd != nil && n.crawlerCmd.Process != nil {
		// Check if process exists
		if err := n.crawlerCmd.Process.Signal(syscall.Signal(0)); err != nil {
			log.Printf("Crawler process signal check failed: %v", err)
			n.stats.CrawlerStatus = "stopped"
		} else {
			n.stats.CrawlerStatus = "running"
		}
		n.stats.CrawlerPid = n.crawlerCmd.Process.Pid
	} else {
		n.stats.CrawlerStatus = "stopped"
		n.stats.CrawlerPid = 0
	}
	
	// Update analyzer status
	if n.analyzerCmd != nil && n.analyzerCmd.Process != nil {
		// Check if process exists
		if err := n.analyzerCmd.Process.Signal(syscall.Signal(0)); err != nil {
			n.stats.AnalyzerStatus = "stopped"
		} else {
			n.stats.AnalyzerStatus = "running"
		}
		n.stats.AnalyzerPid = n.analyzerCmd.Process.Pid
	} else {
		n.stats.AnalyzerStatus = "stopped"
		n.stats.AnalyzerPid = 0
	}
	
	// Update batch size
	n.stats.BatchSize = len(n.analyzerBatch)
	
	// Write stats to file
	if n.config.StatusFile != "" {
		data, err := json.MarshalIndent(n.stats, "", "  ")
	// Check if the table exists before updating
	var tableName string
	err := n.sqliteDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'").Scan(&tableName)
	if err != nil {
		if err == sql.ErrNoRows {
			// Table doesn't exist yet, can't process batch
			log.Printf("Warning: Cannot process batch, pages table doesn't exist yet")
			return
		}
		log.Printf("Error checking for pages table: %v", err)
		return
	}

	// Check if analyzed column exists
	var hasAnalyzedColumn bool = false
	rows, err := n.sqliteDB.Query("PRAGMA table_info(pages)")
	if err != nil {
		log.Printf("Error querying table schema: %v", err)
		return
	}
	defer rows.Close()
	
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype, dfltValue string
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			log.Printf("Error scanning column info: %v", err)
			return
		}
		if name == "analyzed" {
			hasAnalyzedColumn = true
			break
		}
	}
	
	if !hasAnalyzedColumn {
		log.Printf("Warning: 'analyzed' column doesn't exist in pages table, adding it")
		_, err := n.sqliteDB.Exec("ALTER TABLE pages ADD COLUMN analyzed INTEGER DEFAULT 0")
		if err != nil {
			log.Printf("Error adding analyzed column: %v", err)
			return
		}
	}

	// Begin transaction
	tx, err := n.sqliteDB.Begin()
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return
	}

	// Update URLs in batch
	stmt, err := tx.Prepare("UPDATE pages SET analyzed = 1 WHERE url = ?")
	if err != nil {
		tx.Rollback()
		log.Printf("Error preparing statement: %v", err)
		return
	}
	defer stmt.Close()

	for _, url := range n.analyzerBatch {
		if _, err := stmt.Exec(url); err != nil {
			tx.Rollback()
			log.Printf("Error updating URL %s: %v", url, err)
			return
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		return
	}

	// Clear batch
	n.analyzerBatch = n.analyzerBatch[:0]
}

// readConfig reads the YAML configuration file
func readConfig(configPath string) (Config, error) {
	// Default configuration
	config := Config{
		PostgresURL:    "postgres://localhost:5432/ndaivi?sslmode=disable",
		SQLiteDBPath:   "/var/ndaivimanuales/scraper/data/crawler.db",
		MaxBatchSize:   32,
		MaxUnanalyzedURLs: 1024,
		SpeedCheckInterval: 5,
		TargetWebsite:  "manualslib.com", // Default target website
		MaxUrls:        1000, // Default max URLs
		CrawlerScript: "/var/ndaivimanuales/scraper/web_crawler.py",
		AnalyzerScript: "/var/ndaivimanuales/scraper/claude_analyzer.py", // Fixed analyzer path
		StatusFile:    "/var/ndaivimanuales/logs/ndaivi_status.json",
		LogFile:       "/var/ndaivimanuales/logs/ndaivi.log",
		PidFile:       "/var/ndaivimanuales/logs/ndaivi.pid", // Changed to logs directory for permissions
	}

	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Printf("Warning: Could not read config file %s: %v. Using defaults.", configPath, err)
		return config, nil
	}

	// Parse YAML
	var yamlConfig map[string]interface{}
	if err := yaml.Unmarshal(data, &yamlConfig); err != nil {
		log.Printf("Warning: Could not parse config file %s: %v. Using defaults.", configPath, err)
		return config, nil
	}

	// Extract database configuration
	if db, ok := yamlConfig["database"].(map[string]interface{}); ok {
		host := getStringOrDefault(db, "host", "localhost")
		port := getIntOrDefault(db, "port", 5432)
		user := getStringOrDefault(db, "user", "postgres")
		password := getStringOrDefault(db, "password", "postgres")
		dbname := getStringOrDefault(db, "dbname", "ndaivi")
		sslmode := getStringOrDefault(db, "sslmode", "disable")

		config.PostgresURL = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", 
			user, password, host, port, dbname, sslmode)
	}

	// Extract web crawler configuration
	if wc, ok := yamlConfig["web_crawler"].(map[string]interface{}); ok {
		config.SQLiteDBPath = getStringOrDefault(wc, "db_path", config.SQLiteDBPath)
		config.MaxUnanalyzedURLs = getIntOrDefault(wc, "max_unanalyzed_urls", config.MaxUnanalyzedURLs)
		config.SpeedCheckInterval = getIntOrDefault(wc, "crawl_speed_check_interval", config.SpeedCheckInterval)
		config.TargetWebsite = getStringOrDefault(wc, "target_website", "")
		config.MaxUrls = getIntOrDefault(wc, "max_urls", 1000) // Default to 1000 if not specified
	}

	return config, nil
}

// Helper functions for config parsing
func getStringOrDefault(m map[string]interface{}, key, defaultValue string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return defaultValue
}

func getIntOrDefault(m map[string]interface{}, key string, defaultValue int) int {
	switch v := m[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	default:
		return defaultValue
	}
}

func main() {
	// Parse command line arguments
	configPath := flag.String("config", "/var/ndaivimanuales/config.yaml", "Path to config file")
	daemonize := flag.Bool("daemon", false, "Run as daemon")
	startDaemon := flag.Bool("start-daemon", false, "Start in daemon mode") // Added for direct daemon start
	flag.Parse()
	
	// If start-daemon is provided, set daemonize to true
	if *startDaemon {
		*daemonize = true
	}

	// Read configuration
	config, err := readConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to read configuration: %v", err)
	}

	// If running as daemon, redirect output to log file
	if *daemonize {
		// Create log directory if it doesn't exist
		logDir := filepath.Dir(config.LogFile)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}

		// Open log file
		logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		log.SetOutput(logFile)
		stdout, err := logFile.Stat()
		if err == nil {
			os.Stdout = os.NewFile(logFile.Fd(), stdout.Name())
			os.Stderr = os.NewFile(logFile.Fd(), stdout.Name())
		}
		
		// Detach from terminal if not already a daemon (ppid != 1)
		// and not explicitly started with --start-daemon
		if os.Getppid() != 1 && !*startDaemon {
			// Re-run the same command as background process with nohup
			cmd := exec.Command("nohup", os.Args[0], "--start-daemon", "--config", *configPath)
			cmd.Start()
			fmt.Printf("NDAIVI daemon started with PID %d\n", cmd.Process.Pid)
			os.Exit(0)
		}
		
		// We're now running as a daemon, create PID file
		pid := os.Getpid()
		log.Printf("Writing PID %d to %s", pid, config.PidFile)
		if err := os.WriteFile(config.PidFile, []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
			log.Printf("Warning: Failed to write PID file: %v", err)
		}
	}
	
	// Configure status file
	statusDir := filepath.Dir(config.StatusFile)
	if err := os.MkdirAll(statusDir, 0755); err != nil {
		log.Fatalf("Failed to create status directory: %v", err)
	}

	// Create NDAIVI instance
	ndaivi, err := NewNDAIVI(config)
	if err != nil {
		log.Fatalf("Failed to create NDAIVI instance: %v", err)
	}

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Start system
	if err := ndaivi.Start(); err != nil {
		log.Fatalf("Failed to start NDAIVI: %v", err)
	}

	// Wait for signal
	<-sigChan
	ndaivi.Stop()
}
