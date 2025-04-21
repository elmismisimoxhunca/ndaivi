package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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

// Config holds all configuration data for the application
type Config struct {
	// Database configuration
	PostgresURL    string `yaml:"PostgresURL"`
	SQLiteDBPath   string `yaml:"SQLiteDBPath"`
	
	// System limits and settings
	MaxBatchSize   int    `yaml:"MaxBatchSize"`
	MaxConcurrentAnalysis int `yaml:"MaxConcurrentAnalysis"`
	LinkCheckInterval int `yaml:"LinkCheckInterval"`
	BatchInterval  int    `yaml:"BatchInterval"`
	RetryInterval int     `yaml:"RetryInterval"`
	
	// Target website configuration
	TargetWebsite  string `yaml:"TargetWebsite"`
	MaxUrls        int    `yaml:"MaxUrls"`
	
	// Script paths
	CrawlerScript  string `yaml:"CrawlerScript"`
	AnalyzerScript string `yaml:"AnalyzerScript"`
	
	// System files
	StatusFile     string `yaml:"StatusFile"`
	PidFile        string `yaml:"PidFile"`
	TempDir        string `yaml:"TempDir"`
	
	// Log files
	MainLogFile    string `yaml:"MainLogFile"`
	CrawlerLogFile string `yaml:"CrawlerLogFile"`
	AnalyzerLogFile string `yaml:"AnalyzerLogFile"`
}

// Stats contains runtime statistics for the system
type Stats struct {
	// Link statistics
	TotalURLs       int    `json:"total_urls"`
	AnalyzedURLs    int    `json:"analyzed_urls"`
	UnanalyzedURLs  int    `json:"unanalyzed_urls"`
	
	// Component status
	CrawlerStatus   string `json:"crawler_status"`
	AnalyzerStatus  string `json:"analyzer_status"`
	BatchSize       int    `json:"batch_size"`
	
	// System information
	LastUpdated     string `json:"last_updated"`
	Progress        int    `json:"progress"`
	TargetUrls      int    `json:"target_urls"`
	CrawlerPid      int    `json:"crawler_pid"`
	AnalyzerPid     int    `json:"analyzer_pid"`
	
	// Log files
	CrawlerLogFile  string `json:"crawler_log_file"`
	AnalyzerLogFile string `json:"analyzer_log_file"`
	MainLogFile     string `json:"main_log_file"`
}

// LinkStatus represents the full link status tracking file
type LinkStatus struct {
	LastUpdated string `json:"last_updated"`
	Links       []Link `json:"links"`
}

// Link represents a single URL with its analysis status
type Link struct {
	URL        string `json:"url"`
	Priority   int    `json:"priority"`
	Analyzed   bool   `json:"analyzed"`
	Error      string `json:"error,omitempty"`
	AddedAt    string `json:"added_at"`
	AnalyzedAt string `json:"analyzed_at,omitempty"`
}

// NDAIVI is the main application structure
type NDAIVI struct {
	config         Config
	pgDB           *sql.DB
	sqliteDB       *sql.DB
	crawlerCmd     *exec.Cmd
	analyzerCmd    *exec.Cmd
	linkStatusPath string
	linkStatus     *LinkStatus
	stats          Stats
	stopChan       chan struct{}
	wg             sync.WaitGroup
	mu             sync.Mutex
	
	// Loggers
	mainLogger     *log.Logger
	crawlerLogger  *log.Logger
	analyzerLogger *log.Logger
}

// NewNDAIVI creates a new NDAIVI instance
func NewNDAIVI(config Config) (*NDAIVI, error) {
	// Create directory structure for logs
	for _, dir := range []string{
		filepath.Dir(config.MainLogFile),
		filepath.Dir(config.CrawlerLogFile),
		filepath.Dir(config.AnalyzerLogFile),
		filepath.Dir(config.StatusFile),
		filepath.Dir(config.PidFile),
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Setup main logger
	mainLogFile, err := os.OpenFile(config.MainLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open main log file: %w", err)
	}
	mainLogger := log.New(io.MultiWriter(os.Stdout, mainLogFile), "[MAIN] ", log.LstdFlags)
	
	// Setup crawler logger
	crawlerLogFile, err := os.OpenFile(config.CrawlerLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open crawler log file: %w", err)
	}
	crawlerLogger := log.New(crawlerLogFile, "[CRAWLER] ", log.LstdFlags)
	
	// Setup analyzer logger
	analyzerLogFile, err := os.OpenFile(config.AnalyzerLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open analyzer log file: %w", err)
	}
	analyzerLogger := log.New(analyzerLogFile, "[ANALYZER] ", log.LstdFlags)

	// Connect to PostgreSQL database
	mainLogger.Println("Connecting to PostgreSQL database...")
	pgDB, err := sql.Open("postgres", config.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	
	// Verify PostgreSQL connection
	if err := pgDB.Ping(); err != nil {
		mainLogger.Printf("Warning: PostgreSQL connection not verified: %v", err)
	} else {
		mainLogger.Println("PostgreSQL connection verified")
	}

	// Connect to SQLite database (read-only, as per WindsurfRules)
	mainLogger.Println("Connecting to SQLite database in read-only mode...")
	sqliteDB, err := sql.Open("sqlite3", "file:"+config.SQLiteDBPath+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite: %w", err)
	}
	
	// Initialize runtime stats
	stats := Stats{
		CrawlerStatus:   "stopped",
		AnalyzerStatus:  "stopped",
		LastUpdated:     time.Now().Format(time.RFC3339),
		TargetUrls:      config.MaxUrls,
		CrawlerLogFile:  config.CrawlerLogFile,
		AnalyzerLogFile: config.AnalyzerLogFile,
		MainLogFile:     config.MainLogFile,
	}

	mainLogger.Println("NDAIVI instance created successfully")

	// Set up link status path (default to data directory)
	linkStatusPath := filepath.Join(filepath.Dir(config.SQLiteDBPath), "link_status.json")
	
	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(linkStatusPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create link status directory: %w", err)
	}
	
	// Create NDAIVI instance
	ndaivi := &NDAIVI{
		config:         config,
		pgDB:           pgDB,
		sqliteDB:       sqliteDB,
		linkStatusPath: linkStatusPath,
		linkStatus:     &LinkStatus{
			LastUpdated: time.Now().Format(time.RFC3339),
			Links:       []Link{},
		},
		stats:          stats,
		stopChan:       make(chan struct{}),
		mainLogger:     mainLogger,
		crawlerLogger:  crawlerLogger,
		analyzerLogger: analyzerLogger,
	}
	
	// Load existing link status if available
	if err := ndaivi.loadLinkStatus(); err != nil {
		mainLogger.Printf("Warning: Failed to load link status: %v", err)
	}
	
	return ndaivi, nil
}

// Start initializes and starts all components of the NDAIVI system
func (n *NDAIVI) Start() error {
	n.mainLogger.Println("Starting NDAIVI system...")

	// Start crawler first
	n.mainLogger.Println("Starting web crawler to populate database...")
	if err := n.startCrawler(); err != nil {
		return fmt.Errorf("failed to start crawler: %w", err)
	}

	// Start monitoring routines
	n.wg.Add(4)
	go n.monitorLinks()          // Monitor for new links in SQLite
	go n.processLinkBatches()    // Send batches to analyzer
	go n.processAnalyzerOutput() // Process analyzer results
	go n.updateStats()           // Update system statistics

	// Wait for some data to be available before starting analyzer
	n.mainLogger.Println("Waiting for crawler to populate database before starting analyzer...")
	time.Sleep(10 * time.Second) // Give crawler time to populate some data

	// Check if we have any data to analyze
	hasData, err := n.checkForNewLinks()
	if err != nil {
		n.mainLogger.Printf("Warning: Error checking for crawled data: %v", err)
	}

	if !hasData {
		n.mainLogger.Println("No data found in database yet. Will start analyzer anyway.")
	} else {
		n.mainLogger.Println("Found data in database. Starting analyzer...")
	}

	// Start analyzer
	if err := n.startAnalyzer(); err != nil {
		return fmt.Errorf("failed to start analyzer: %w", err)
	}

	return nil
}

// Stop gracefully shuts down all components of the NDAIVI system
func (n *NDAIVI) Stop() {
	n.mainLogger.Println("Stopping NDAIVI system...")
	close(n.stopChan)
	
	// Stop crawler process
	if n.crawlerCmd != nil && n.crawlerCmd.Process != nil {
		n.mainLogger.Println("Sending SIGTERM to crawler process...")
		n.crawlerCmd.Process.Signal(syscall.SIGTERM)
		
		// Wait for process to terminate
		go func() {
			// Set a timeout for process termination
			timeout := time.After(5 * time.Second)
			done := make(chan error, 1)
			
			go func() {
				done <- n.crawlerCmd.Wait()
			}()
			
			select {
			case <-timeout:
				// Process didn't terminate in time, force kill
				n.mainLogger.Println("Crawler process didn't terminate in time, killing...")
				n.crawlerCmd.Process.Kill()
			case err := <-done:
				if err != nil {
					n.mainLogger.Printf("Crawler process terminated with error: %v", err)
				} else {
					n.mainLogger.Println("Crawler process terminated successfully")
				}
			}
		}()
	}

	// Stop analyzer process if running
	if n.analyzerCmd != nil && n.analyzerCmd.Process != nil {
		n.mainLogger.Println("Sending SIGTERM to analyzer process...")
		n.analyzerCmd.Process.Signal(syscall.SIGTERM)
		
		// Wait for process to terminate
		go func() {
			// Set a timeout for process termination
			timeout := time.After(5 * time.Second)
			done := make(chan error, 1)
			
			go func() {
				done <- n.analyzerCmd.Wait()
			}()
			
			select {
			case <-timeout:
				// Process didn't terminate in time, force kill
				n.mainLogger.Println("Analyzer process didn't terminate in time, killing...")
				n.analyzerCmd.Process.Kill()
			case err := <-done:
				if err != nil {
					n.mainLogger.Printf("Analyzer process terminated with error: %v", err)
				} else {
					n.mainLogger.Println("Analyzer process terminated successfully")
				}
			}
		}()
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

	// Kill any other child processes that might have been started
	n.mainLogger.Println("Checking for any remaining child processes...")
	findAndKillChildProcesses(os.Getpid())
	
	// Remove PID file if it exists and belongs to us
	if n.config.PidFile != "" {
		if data, err := os.ReadFile(n.config.PidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid == os.Getpid() {
				os.Remove(n.config.PidFile)
				n.mainLogger.Println("PID file removed")
			}
		}
	}

	n.mainLogger.Println("NDAIVI system stopped")
}

// startCrawler starts the web crawler process
func (n *NDAIVI) startCrawler() error {
	n.mainLogger.Println("Starting web crawler...")

	// Check if crawler script exists
	n.mainLogger.Printf("Looking for crawler script at path: %s", n.config.CrawlerScript)
	if _, err := os.Stat(n.config.CrawlerScript); os.IsNotExist(err) {
		return fmt.Errorf("crawler script not found at %s", n.config.CrawlerScript)
	}

	// Pass target website and other config via environment variables
	env := os.Environ()
	env = append(env, fmt.Sprintf("NDAIVI_TARGET_WEBSITE=%s", n.config.TargetWebsite))
	env = append(env, fmt.Sprintf("NDAIVI_MAX_URLS=%d", n.config.MaxUrls))
	env = append(env, fmt.Sprintf("NDAIVI_DB_PATH=%s", n.config.SQLiteDBPath))
	env = append(env, fmt.Sprintf("NDAIVI_LOG_FILE=%s", n.config.CrawlerLogFile))

	// Create command
	n.mainLogger.Printf("Executing python3 %s", n.config.CrawlerScript)
	cmd := exec.Command("python3", n.config.CrawlerScript)
	cmd.Env = env

	// Get pipes for stdout and stderr
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start crawler: %w", err)
	}
	
	// Store command and update status
	n.crawlerCmd = cmd
	n.stats.CrawlerStatus = "running"
	n.stats.CrawlerPid = cmd.Process.Pid

	// Log crawler output in background
	n.wg.Add(2)
	go n.streamOutput(stdoutPipe, n.crawlerLogger, "STDOUT")
	go n.streamOutput(stderrPipe, n.crawlerLogger, "STDERR")

	// Monitor crawler process in background
	go func() {
		err := cmd.Wait()
		n.mu.Lock()
		n.stats.CrawlerStatus = "stopped"
		n.stats.CrawlerPid = 0
		n.mu.Unlock()
		
		if err != nil {
			n.mainLogger.Printf("Crawler process exited with error: %v", err)
		} else {
			n.mainLogger.Println("Crawler process exited successfully")
		}
	}()

	n.mainLogger.Printf("Crawler started with PID %d", cmd.Process.Pid)
	return nil
}

// startAnalyzer starts the analyzer process
func (n *NDAIVI) startAnalyzer() error {
	n.mainLogger.Println("Starting analyzer...")

	// Use the configured analyzer script from config.yaml
	analyzerScript := n.config.AnalyzerScript
	n.mainLogger.Printf("Using analyzer script from config: %s", analyzerScript)
	
	// Check if analyzer script exists and is executable
	fileInfo, err := os.Stat(analyzerScript)
	if os.IsNotExist(err) {
		n.mainLogger.Printf("ERROR: Analyzer script not found at %s", analyzerScript)
		return fmt.Errorf("analyzer script not found at %s", analyzerScript)
	}
	
	// Log file permissions
	n.mainLogger.Printf("Analyzer script permissions: %s", fileInfo.Mode().String())

	// Pass configuration via environment variables
	env := os.Environ()
	env = append(env, fmt.Sprintf("NDAIVI_LOG_FILE=%s", n.config.AnalyzerLogFile))
	env = append(env, fmt.Sprintf("NDAIVI_DB_PATH=%s", n.config.SQLiteDBPath))
	
	// Always include the project root in PYTHONPATH
	env = append(env, "PYTHONPATH=/var/ndaivimanuales")

	// Use the virtual environment python if it exists
	venvPython := "/var/ndaivimanuales/venv/bin/python"
	venvExists := false
	
	// Check if the virtual environment exists
	if _, err := os.Stat(venvPython); !os.IsNotExist(err) {
		venvExists = true
		n.mainLogger.Printf("Found virtual environment Python at %s", venvPython)
	} else {
		// Fall back to system Python if venv doesn't exist
		venvPython = "python3"
		n.mainLogger.Printf("Virtual environment Python not found, using system Python: %s", venvPython)
	}
	
	// Properly configure virtual environment
	if venvExists {
		env = append(env, fmt.Sprintf("VIRTUAL_ENV=%s", "/var/ndaivimanuales/venv"))
		
		// Add the venv bin directory to the front of PATH
		venvPath := fmt.Sprintf("/var/ndaivimanuales/venv/bin:%s", os.Getenv("PATH"))
		env = append(env, fmt.Sprintf("PATH=%s", venvPath))
		
		n.mainLogger.Printf("Virtual environment activated with PATH=%s", venvPath)
	}
	
	// Run the analyzer script directly (not as a module)
	n.mainLogger.Printf("Executing %s %s (cwd: /var/ndaivimanuales/scraper)", venvPython, analyzerScript)
	cmd := exec.Command(venvPython, analyzerScript)
	cmd.Dir = "/var/ndaivimanuales/scraper"
	// Set command environment with our configuration variables
	cmd.Env = env
	
	// Setup stdout and stderr pipes
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the command
	n.mainLogger.Printf("Attempting to start analyzer command: %s %s", venvPython, analyzerScript)
	if err := cmd.Start(); err != nil {
		n.mainLogger.Printf("ERROR: Failed to start analyzer: %v", err)
		return fmt.Errorf("failed to start analyzer: %w", err)
	}
	
	// Store command and update status
	n.analyzerCmd = cmd
	n.stats.AnalyzerStatus = "running"
	n.stats.AnalyzerPid = cmd.Process.Pid
	n.mainLogger.Printf("Analyzer process started successfully with PID: %d", cmd.Process.Pid)

	// Log both stdout and stderr in background
	n.wg.Add(2)
	go n.streamOutput(stdoutPipe, n.analyzerLogger, "STDOUT")
	go n.streamOutput(stderrPipe, n.analyzerLogger, "STDERR")

	// Monitor analyzer process in background
	go func() {
		err := cmd.Wait()
		n.mu.Lock()
		n.stats.AnalyzerStatus = "stopped"
		n.stats.AnalyzerPid = 0
		n.mu.Unlock()
		
		if err != nil {
			n.mainLogger.Printf("Analyzer process exited with error: %v", err)
		} else {
			n.mainLogger.Println("Analyzer process exited successfully")
		}
	}()

	n.mainLogger.Printf("Analyzer started with PID %d", cmd.Process.Pid)
	return nil
}

// streamOutput reads from a pipe and logs the output
func (n *NDAIVI) streamOutput(pipe io.ReadCloser, logger *log.Logger, prefix string) {
	defer n.wg.Done()
	
	// Create scanner to read line by line
	scanner := bufio.NewScanner(pipe)
	
	// Read and log each line
	for scanner.Scan() {
		logger.Printf("%s: %s", prefix, scanner.Text())
	}
	
	if err := scanner.Err(); err != nil {
		logger.Printf("Error reading %s: %v", prefix, err)
	}
}

// loadLinkStatus loads the link status from a JSON file
func (n *NDAIVI) loadLinkStatus() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	n.mainLogger.Printf("Loading link status from %s", n.linkStatusPath)
	
	// Check if file exists, create if not
	if _, err := os.Stat(n.linkStatusPath); os.IsNotExist(err) {
		// Create new empty link status file
		n.linkStatus = &LinkStatus{
			LastUpdated: time.Now().Format(time.RFC3339),
			Links:       []Link{},
		}
		return n.saveLinkStatus()
	}
	
	// Read file
	data, err := os.ReadFile(n.linkStatusPath)
	if err != nil {
		return fmt.Errorf("failed to read link status file: %w", err)
	}
	
	// Parse JSON
	var linkStatus LinkStatus
	if err := json.Unmarshal(data, &linkStatus); err != nil {
		return fmt.Errorf("failed to parse link status file: %w", err)
	}
	
	n.linkStatus = &linkStatus
	n.mainLogger.Printf("Loaded %d links from status file", len(n.linkStatus.Links))
	return nil
}

// saveLinkStatus saves the link status to a JSON file
func (n *NDAIVI) saveLinkStatus() error {
	// Update timestamp
	n.linkStatus.LastUpdated = time.Now().Format(time.RFC3339)
	
	// Marshal to JSON
	data, err := json.MarshalIndent(n.linkStatus, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal link status: %w", err)
	}
	
	// Create backup of existing file if it exists
	if _, err := os.Stat(n.linkStatusPath); err == nil {
		backupPath := n.linkStatusPath + ".bak"
		if err := os.Rename(n.linkStatusPath, backupPath); err != nil {
			n.mainLogger.Printf("Warning: failed to create backup of link status file: %v", err)
		}
	}
	
	// Write to file
	if err := os.WriteFile(n.linkStatusPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write link status file: %w", err)
	}
	
	n.mainLogger.Printf("Saved %d links to status file", len(n.linkStatus.Links))
	return nil
}

// monitorLinks periodically checks for new links in the SQLite database
func (n *NDAIVI) monitorLinks() {
	defer n.wg.Done()
	n.mainLogger.Println("Starting link monitoring...")
	
	// Check for new links immediately at startup
	n.mainLogger.Println("Performing initial check for links in SQLite database...")
	found, err := n.checkForNewLinks()
	if err != nil {
		n.mainLogger.Printf("Error checking for new links at startup: %v", err)
	} else if found {
		n.mainLogger.Println("Initial links found and added to link status file")
	} else {
		n.mainLogger.Println("No initial links found in database, will check again in 5 seconds")
	}
	
	// Create a ticker for periodic checks - always use 5 seconds regardless of config
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			n.mainLogger.Println("Link monitoring stopped")
			return
		case <-ticker.C:
			// Check for new links in SQLite (read-only mode)
			found, err := n.checkForNewLinks()
			if err != nil {
				n.mainLogger.Printf("Error checking for new links: %v", err)
			} else if found {
				n.mainLogger.Println("New links found and added to link status file")
			}
		}
	}
}

// checkForNewLinks checks for new links in the SQLite database (read-only) and adds them to the link status
// Returns true if links were found, false otherwise
func (n *NDAIVI) checkForNewLinks() (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	n.mainLogger.Println("Checking for new links in SQLite database...")
	
	// Ensure we have the latest link status
	if err := n.loadLinkStatus(); err != nil {
		n.mainLogger.Printf("Error loading link status: %v", err)
		return false, err
	}
	
	// Check if SQLite connection is active
	if n.sqliteDB == nil {
		n.mainLogger.Println("SQLite database connection not available")
		return false, fmt.Errorf("SQLite database connection not available")
	}
	
	// Get existing URLs from link status
	existingURLs := make(map[string]bool)
	for _, link := range n.linkStatus.Links {
		existingURLs[link.URL] = true
	}
	
	// Check if the pages table exists
	var tableName string
	err := n.sqliteDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'").Scan(&tableName)
	if err != nil {
		if err == sql.ErrNoRows {
			n.mainLogger.Println("pages table doesn't exist yet in SQLite database")
			return false, nil
		}
		n.mainLogger.Printf("Error checking for pages table: %v", err)
		return false, err
	}

	// Query SQLite for new links
	n.mainLogger.Println("Querying for new URLs from pages table")
	
	// Per WindsurfRule #3: No ANALYSED flag in SQLite. Link status is tracked in a separate JSON file.
	// We need to get URLs from pages table that aren't in our link_status.json
	rows, err := n.sqliteDB.Query(`
		SELECT url, status_code 
		FROM pages 
		LIMIT ?`, n.config.MaxBatchSize)
	if err != nil {
		n.mainLogger.Printf("Error querying SQLite pages table: %v", err)
		return false, fmt.Errorf("failed to query SQLite: %w", err)
	}
	defer rows.Close()

	// Process results and add new URLs to link status
	newLinks := 0
	for rows.Next() {
		var url string
		var statusCode int
		
		if err := rows.Scan(&url, &statusCode); err != nil {
			n.mainLogger.Printf("Error scanning row: %v", err)
			continue
		}
		
		// Debug log
		n.mainLogger.Printf("Found URL in database: %s (status: %d)", url, statusCode)
		
		// Skip if URL already exists in our tracking
		if existingURLs[url] {
			continue
		}
		
		// Calculate a priority score based on URL (simple heuristic)
		priority := 100
		if strings.Contains(url, n.config.TargetWebsite) {
			priority += 100 // Boost priority for main target website
		}
		
		// Add new URL to link status file
		n.linkStatus.Links = append(n.linkStatus.Links, Link{
			URL:       url,
			Priority:  priority,
			Analyzed:  false,
			AddedAt:   time.Now().Format(time.RFC3339),
		})
		
		newLinks++
	}
	
	if err := rows.Err(); err != nil {
		n.mainLogger.Printf("Error iterating rows: %v", err)
		return false, err
	}

	// Save link status if we added new links
	if newLinks > 0 {
		n.mainLogger.Printf("Added %d new links from SQLite database", newLinks)
		if err := n.saveLinkStatus(); err != nil {
			n.mainLogger.Printf("Error saving link status: %v", err)
		}
		
		// Update stats
		n.stats.TotalURLs += newLinks
		n.stats.UnanalyzedURLs += newLinks
		
		// Return true to indicate that links were found
		return true, nil
	} else {
		n.mainLogger.Println("No new links found in SQLite database")
		
		// Return false to indicate that no links were found
		return false, nil
	}
}

// prepareAnalyzerBatch selects a batch of links from the link status file for analysis
func (n *NDAIVI) prepareAnalyzerBatch() ([]Link, error) {
	// First ensure we have the latest link status
	if err := n.loadLinkStatus(); err != nil {
		return nil, fmt.Errorf("failed to load link status: %w", err)
	}
	
	// Look for unanalyzed links
	var batch []Link
	for _, link := range n.linkStatus.Links {
		if !link.Analyzed && link.Error == "" {
			// Add to batch
			batch = append(batch, link)
			
			// Stop when we reach the max batch size
			if len(batch) >= n.config.MaxBatchSize {
				break
			}
		}
	}
	
	return batch, nil
}

// processAnalyzerOutput processes output from the analyzer
func (n *NDAIVI) processAnalyzerOutput() {
	defer n.wg.Done()
	n.mainLogger.Println("Starting analyzer output processing...")
	
	// We'll wait for results via files created by the analyzer
	// and process them in a loop
	n.mainLogger.Println("Waiting for analysis results from the analyzer...")
	
	// Create a ticker to check the results directory periodically
	resultsDir := filepath.Join(n.config.TempDir, "analyzer_results")
	
	// Ensure results directory exists
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		n.mainLogger.Printf("Error creating results directory: %v", err)
		return
	}
	
	// Create ticker for periodic checks
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-n.stopChan:
			n.mainLogger.Println("Analyzer output processing stopped")
			return
		case <-ticker.C:
			// List all result files
			files, err := os.ReadDir(resultsDir)
			if err != nil {
				n.mainLogger.Printf("Error reading results directory: %v", err)
				continue
			}
			
			// Process each result file
			for _, file := range files {
				if !file.IsDir() && strings.HasPrefix(file.Name(), "result_") && strings.HasSuffix(file.Name(), ".json") {
					filePath := filepath.Join(resultsDir, file.Name())
					
					// Read result file
					data, err := os.ReadFile(filePath)
					if err != nil {
						n.mainLogger.Printf("Error reading result file %s: %v", file.Name(), err)
						continue
					}
					
					// Parse result
					var result struct {
						URL    string                 `json:"url"`
						Status string                 `json:"status"`
						Data   map[string]interface{} `json:"data,omitempty"`
						Error  string                 `json:"error,omitempty"`
					}
					
					if err := json.Unmarshal(data, &result); err != nil {
						n.mainLogger.Printf("Error parsing result file %s: %v", file.Name(), err)
						
						// Move the file to an error directory so we don't keep trying to process it
						errorsDir := filepath.Join(n.config.TempDir, "analyzer_errors")
						if err := os.MkdirAll(errorsDir, 0755); err != nil {
							n.mainLogger.Printf("Error creating errors directory: %v", err)
							continue
						}
						
						if err := os.Rename(filePath, filepath.Join(errorsDir, file.Name())); err != nil {
							n.mainLogger.Printf("Error moving file to errors directory: %v", err)
						}
						continue
					}
					
					// Log the result
					n.analyzerLogger.Printf("Processing analysis result for %s: %s", result.URL, result.Status)
					
					// Update link status in JSON file (not SQLite, which is read-only for Go daemon)
					if err := n.updateLinkStatus(result.URL, result.Status == "success", result.Error); err != nil {
						n.mainLogger.Printf("Error updating link status: %v", err)
						continue
					}
					
					// If analysis was successful, push results to PostgreSQL
					if result.Status == "success" && result.Data != nil {
						if err := n.pushResultToPostgres(result.URL, result.Data); err != nil {
							n.mainLogger.Printf("Error pushing result to PostgreSQL: %v", err)
						} else {
							n.mainLogger.Printf("Successfully pushed result to PostgreSQL for URL: %s", result.URL)
						}
					}
					
					// Update stats
					n.mu.Lock()
					n.stats.AnalyzedURLs++
					n.stats.UnanalyzedURLs--
					if n.stats.UnanalyzedURLs < 0 {
						n.stats.UnanalyzedURLs = 0
					}
					n.mu.Unlock()
					
					// Delete the processed file
					if err := os.Remove(filePath); err != nil {
						n.mainLogger.Printf("Error removing processed file %s: %v", filePath, err)
					}
				}
			}
		}
	}
}

// updateLinkStatus updates the link status file to mark a URL as analyzed
func (n *NDAIVI) updateLinkStatus(url string, success bool, errorMsg string) error {
	// Lock to prevent concurrent modification
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// Ensure we have the latest link status
	if err := n.loadLinkStatus(); err != nil {
		return fmt.Errorf("failed to load link status: %w", err)
	}
	
	// Find the URL in the link status file
	found := false
	for i := range n.linkStatus.Links {
		if n.linkStatus.Links[i].URL == url {
			// Update the link status
			n.linkStatus.Links[i].Analyzed = true
			n.linkStatus.Links[i].AnalyzedAt = time.Now().Format(time.RFC3339)
			
			// Set error message if analysis failed
			if !success {
				n.linkStatus.Links[i].Error = errorMsg
			}
			
			found = true
			break
		}
	}
	
	// If URL not found, add it to the link status file
	if !found {
		n.mainLogger.Printf("Warning: URL %s not found in link status file", url)
		
		// Add the URL to the link status file anyway
		n.linkStatus.Links = append(n.linkStatus.Links, Link{
			URL:        url,
			Priority:   0,
			Analyzed:   true,
			AddedAt:    time.Now().Format(time.RFC3339),
			AnalyzedAt: time.Now().Format(time.RFC3339),
			Error:      errorMsg,
		})
	}
	
	// Save the link status file
	return n.saveLinkStatus()
}

// pushResultToPostgres pushes the analysis result to PostgreSQL
func (n *NDAIVI) pushResultToPostgres(url string, result map[string]interface{}) error {
	// Check if PostgreSQL connection is active
	if n.pgDB == nil {
		return fmt.Errorf("PostgreSQL database connection not available")
	}
	
	n.mainLogger.Printf("Pushing analysis result for %s to PostgreSQL", url)
	
	// Convert result to JSON for storage
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}
	
	// Insert into PostgreSQL
	_, err = n.pgDB.Exec(`
		INSERT INTO analysis_results (url, result, analyzed_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (url) DO UPDATE 
		SET result = $2, analyzed_at = $3`, 
		url, resultJSON, time.Now())
	
	if err != nil {
		return fmt.Errorf("failed to insert result into PostgreSQL: %w", err)
	}
	
	return nil
}

// updateStats periodically updates the system statistics
// processLinkBatches processes batches of links for analysis and sends them to the analyzer
func (n *NDAIVI) processLinkBatches() {
	defer n.wg.Done()
	n.mainLogger.Println("Starting link batch processing...")
	
	// Create a ticker for periodic checks
	ticker := time.NewTicker(time.Duration(n.config.BatchInterval) * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-n.stopChan:
			n.mainLogger.Println("Link batch processing stopped")
			return
		case <-ticker.C:
			// Process a batch of links
			if err := n.processBatch(); err != nil {
				n.mainLogger.Printf("Error processing batch: %v", err)
			}
		}
	}
}

// processBatch processes a single batch of links
func (n *NDAIVI) processBatch() error {
	// Check if analyzer is running
	if n.analyzerCmd == nil || n.analyzerCmd.Process == nil {
		return fmt.Errorf("analyzer not running")
	}
	
	// Prepare a batch of links for analysis
	batch, err := n.prepareAnalyzerBatch()
	if err != nil {
		return fmt.Errorf("failed to prepare analyzer batch: %w", err)
	}
	
	// Skip if batch is empty
	if len(batch) == 0 {
		n.mainLogger.Println("No links to analyze")
		return nil
	}
	
	n.mainLogger.Printf("Sending batch of %d links to analyzer", len(batch))
	
	// Extract URLs from batch
	urls := make([]string, len(batch))
	for i, link := range batch {
		urls[i] = link.URL
	}
	
	// Prepare JSON batch for analyzer
	batchJSON, err := json.Marshal(urls)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %w", err)
	}
	
	// Create a batch file in the analyzer watch directory
	watchDir := filepath.Join(n.config.TempDir, "analyzer_watch")
	if err := os.MkdirAll(watchDir, 0755); err != nil {
		return fmt.Errorf("failed to create watch directory: %w", err)
	}
	
	// Write batch file directly to watch directory
	batchFile := filepath.Join(watchDir, fmt.Sprintf("batch_%d.json", time.Now().UnixNano()))
	if err := os.WriteFile(batchFile, batchJSON, 0644); err != nil {
		return fmt.Errorf("failed to write batch file: %w", err)
	}
	
	n.mainLogger.Printf("Created batch file %s for analyzer", batchFile)
	return nil
}



func (n *NDAIVI) updateStats() {
	defer n.wg.Done()
	n.mainLogger.Println("Starting stats monitoring...")

	// Create a ticker for periodic updates
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			n.mainLogger.Println("Stats monitoring stopped")
			return
		case <-ticker.C:
			// Update stats
			n.mu.Lock()
			
			// Count links
			if n.linkStatus != nil {
				totalLinks := len(n.linkStatus.Links)
				analyzedLinks := 0
				for _, link := range n.linkStatus.Links {
					if link.Analyzed {
						analyzedLinks++
					}
				}
				
				n.stats.TotalURLs = totalLinks
				n.stats.AnalyzedURLs = analyzedLinks
				n.stats.UnanalyzedURLs = totalLinks - analyzedLinks
			}
			
			// Update timestamp and calculate progress
			n.stats.LastUpdated = time.Now().Format(time.RFC3339)
			if n.stats.TargetUrls > 0 && n.stats.TotalURLs > 0 {
				n.stats.Progress = (n.stats.AnalyzedURLs * 100) / n.stats.TargetUrls
				if n.stats.Progress > 100 {
					n.stats.Progress = 100
				}
			}
			
			// Check component status
			if n.crawlerCmd != nil && n.crawlerCmd.Process != nil {
				n.stats.CrawlerStatus = "running"
				n.stats.CrawlerPid = n.crawlerCmd.Process.Pid
			} else {
				n.stats.CrawlerStatus = "stopped"
				n.stats.CrawlerPid = 0
			}
			
			if n.analyzerCmd != nil && n.analyzerCmd.Process != nil {
				n.stats.AnalyzerStatus = "running"
				n.stats.AnalyzerPid = n.analyzerCmd.Process.Pid
			} else {
				n.stats.AnalyzerStatus = "stopped"
				n.stats.AnalyzerPid = 0
			}
			
			// Save stats to file
			data, err := json.MarshalIndent(n.stats, "", "  ")
			if err != nil {
				n.mainLogger.Printf("Error marshaling stats: %v", err)
			} else {
				if err := os.WriteFile(n.config.StatusFile, data, 0644); err != nil {
					n.mainLogger.Printf("Error writing status file: %v", err)
				}
			}
			
			n.mu.Unlock()
		}
	}
}

// loadConfig loads the application configuration from a YAML file
func loadConfig(configPath string) (Config, error) {
	var config Config
	
	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return config, fmt.Errorf("failed to read config file: %w", err)
	}
	
	// Debug: Print raw YAML content
	fmt.Printf("Raw config file content: %s\n", string(data))
	
	// Parse YAML
	if err := yaml.Unmarshal(data, &config); err != nil {
		return config, fmt.Errorf("failed to parse config file: %w", err)
	}
	
	// Debug: Print parsed config values
	fmt.Printf("Loaded config values:\n")
	fmt.Printf("CrawlerScript: '%s'\n", config.CrawlerScript)
	fmt.Printf("AnalyzerScript: '%s'\n", config.AnalyzerScript)
	
	// Set default values if not specified
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 10
	}
	
	if config.MaxConcurrentAnalysis <= 0 {
		config.MaxConcurrentAnalysis = 5
	}
	
	if config.LinkCheckInterval <= 0 {
		config.LinkCheckInterval = 30 // seconds
	}
	
	if config.BatchInterval <= 0 {
		config.BatchInterval = 10 // seconds
	}
	
	if config.RetryInterval <= 0 {
		config.RetryInterval = 60 // seconds
	}
	
	if config.TempDir == "" {
		config.TempDir = "/tmp/ndaivi"
	}
	
	// Ensure log files are set
	if config.MainLogFile == "" {
		config.MainLogFile = "logs/main.log"
	}
	
	if config.CrawlerLogFile == "" {
		config.CrawlerLogFile = "logs/crawler.log"
	}
	
	if config.AnalyzerLogFile == "" {
		config.AnalyzerLogFile = "logs/analyzer.log"
	}
	
	// Ensure status and pid files are set
	if config.StatusFile == "" {
		config.StatusFile = "status.json"
	}
	
	if config.PidFile == "" {
		config.PidFile = "ndaivi.pid"
	}
	
	return config, nil
}

// main is the entry point for the NDAIVI application
func main() {
	// Define config file path with absolute path
	configPath := "/var/ndaivimanuales/config.yaml"
	
	// Parse command line arguments
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Handle daemon commands
	switch os.Args[1] {
	case "start":
		// Start the daemon
		if err := RunAsDaemon(Start, configPath); err != nil {
			fmt.Printf("Error starting daemon: %v\n", err)
			os.Exit(1)
		}
	
	case "stop":
		// Stop the daemon
		if err := RunAsDaemon(Stop, configPath); err != nil {
			fmt.Printf("Error stopping daemon: %v\n", err)
			os.Exit(1)
		}
	
	case "status":
		// Check daemon status
		if err := RunAsDaemon(Status, configPath); err != nil {
			fmt.Printf("Error checking daemon status: %v\n", err)
			os.Exit(1)
		}
	
	case "run":
		// Run in foreground (called by daemon)
		runNDAIVI(configPath)
	
	default:
		printUsage()
		os.Exit(1)
	}
}

// printUsage prints the usage information
func printUsage() {
	fmt.Println("Usage: ndaivi [command]")
	fmt.Println("Commands:")
	fmt.Println("  start   - Start the NDAIVI daemon")
	fmt.Println("  stop    - Stop the NDAIVI daemon")
	fmt.Println("  status  - Check the status of the NDAIVI daemon")
	fmt.Println("  run     - Run NDAIVI in foreground (for internal use)")
}

// runNDAIVI runs the NDAIVI system in the foreground
func runNDAIVI(configPath string) {
	// Make sure we're using the absolute path to the config file
	if !filepath.IsAbs(configPath) {
		configPath = filepath.Join("/var/ndaivimanuales", configPath)
	}
	
	// Load configuration from YAML file
	config, err := loadConfig(configPath)
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
		os.Exit(1)
	}
	
	// Initialize NDAIVI instance
	ndaivi, err := NewNDAIVI(config)
	if err != nil {
		fmt.Printf("Error initializing NDAIVI: %v\n", err)
		os.Exit(1)
	}
	
	// Start the system
	if err := ndaivi.Start(); err != nil {
		fmt.Printf("Error starting NDAIVI: %v\n", err)
		os.Exit(1)
	}
	
	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	
	// Wait for termination signal
	ndaivi.mainLogger.Println("NDAIVI system running in daemon mode")
	<-sigCh
	
	// Gracefully stop all components
	ndaivi.mainLogger.Println("Received termination signal. Shutting down...")
	ndaivi.Stop()
	ndaivi.mainLogger.Println("NDAIVI system stopped.")
}
