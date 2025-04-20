package coordinator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ndaivi/container/config"
	"github.com/ndaivi/container/database"
)

// Coordinator coordinates the NDAIVI components
type Coordinator struct {
	config         *config.Config
	db            *database.PostgresDB
	crawlerDB      *database.CrawlerDB
	crawlerProcess  *Process
	analyzerProcess *Process

	crawlerStatus struct {
		speedMode     string
		lastUpdated   time.Time
		mutex         sync.Mutex
	}

	stats struct {
		urlsProcessed   int
		urlsQueued      int
		urlsFailed      int
		lastUpdated     time.Time
		mutex           sync.Mutex
	}

	// Analyzed URL batch management
	analyzerBatch struct {
		urls           []string
		maxBatchSize   int
		batchFilePath  string
		mutex          sync.Mutex
	}

	running      bool
	stopCh       chan struct{}
}

// Process represents a Python process
type Process struct {
	cmd    *exec.Cmd
	name   string
	running bool
	paused  bool
	slowMode bool
	mutex  sync.Mutex
}

// NewCoordinator creates a new coordinator
func NewCoordinator(config *config.Config, db *database.PostgresDB) *Coordinator {
	// Create a new coordinator
	coord := &Coordinator{
		config: config,
		db:     db,
		stopCh: make(chan struct{}),
	}

	// Initialize crawler status
	coord.crawlerStatus.speedMode = "normal"
	coord.crawlerStatus.lastUpdated = time.Now()
	
	// Initialize analyzer batch settings
	coord.analyzerBatch.maxBatchSize = 32 // Set batch size to 32 as requested
	coord.analyzerBatch.batchFilePath = "/var/ndaivimanuales/tmp/analyzed_urls_batch.json"
	coord.analyzerBatch.urls = []string{}

	// Initialize crawler database
	dbPath := config.WebCrawler.DBPath
	if dbPath == "" {
		dbPath = "/var/ndaivimanuales/scraper/data/crawler.db"
	}

	// Create the crawler database
	// Connect to crawler database with retry
	var err error
	for i := 0; i < 3; i++ {
		coord.crawlerDB, err = database.NewCrawlerDB(dbPath)
		if err == nil {
			break
		}
		log.Printf("Error connecting to crawler database (attempt %d): %v", i+1, err)
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Printf("Failed to connect to crawler database after 3 attempts: %v", err)
		return nil
	}
	log.Println("Connected to crawler database")

	return coord
}

// Run starts the coordinator
func (c *Coordinator) Run() {
	log.Println("Starting NDAIVI Coordinator")

	// Initialize stop channel
	c.stopCh = make(chan struct{})

	// Create temp directory for batch file if it doesn't exist
	tmpDir := filepath.Dir(c.analyzerBatch.batchFilePath)
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			log.Printf("Warning: Failed to create temp directory for analyzed URL batches: %v", err)
		}
	}

	// Start crawler
	err := c.startCrawler()
	if err != nil {
		log.Printf("Failed to start crawler: %v", err)
	}

	// Start analyzer
	err = c.startAnalyzer()
	if err != nil {
		log.Printf("Failed to start analyzer: %v", err)
	}

	// Start stats collection
	go c.collectStats()

	// Start dynamic crawler speed control
	go c.manageCrawlSpeed()

	// Start monitoring Redis channels
	go func() {
		// TODO: Implement Redis channel monitoring
	}()

	// Mark as running
	c.running = true

	log.Println("NDAIVI Coordinator started")
}

// Stop stops the coordinator
func (c *Coordinator) Stop() {
	log.Println("Stopping coordinator")

	// Set running flag
	c.running = false

	// Close stop channel
	close(c.stopCh)

	// Stop components
	c.stopComponents()

	log.Println("Coordinator stopped")
}

// startComponents starts the Python components
func (c *Coordinator) startComponents() error {
	// Start crawler if enabled
	if c.config.Application.Components.Crawler.Enabled {
		err := c.startCrawler()
		if err != nil {
			return fmt.Errorf("failed to start crawler: %w", err)
		}
	}

	// Start analyzer if enabled
	if c.config.Application.Components.Analyzer.Enabled {
		err := c.startAnalyzer()
		if err != nil {
			return fmt.Errorf("failed to start analyzer: %w", err)
		}
	}

	return nil
}

// startAnalyzer starts the analyzer process
func (c *Coordinator) startAnalyzer() error {
	log.Println("Starting analyzer process")

	// Check if the analyzer is already running
	if c.analyzerProcess != nil && c.analyzerProcess.running {
		log.Println("Analyzer is already running")
		return nil
	}
	
	// Initialize the batch processing
	c.analyzerBatch.mutex.Lock()
	c.analyzerBatch.urls = []string{}
	c.analyzerBatch.mutex.Unlock()

	// Create a new analyzer process
	analyzerProcess := &Process{
		name: "analyzer",
		running: false,
	}

	// Create a Python script to run the analyzer
	script := `
import sys
import json
import time
sys.path.insert(0, '/var/ndaivimanuales')
from scraper.db_manager import DBManager
from scraper.claude_analyzer import ClaudeAnalyzer

# Initialize the analyzer
analyzer = ClaudeAnalyzer()

# Initialize the database manager
db = DBManager()

pending_count = 0
while True:
    try:
        # Get an unanalyzed URL
        urls = db.get_unanalyzed_urls(1)  # Get one URL at a time
        if not urls:
            pending_count += 1
            if pending_count > 10:
                # If we can't find URLs for 10 consecutive tries, wait longer
                print("No unanalyzed URLs available, waiting 30 seconds...")
                sys.stdout.flush()
                time.sleep(30)
                pending_count = 0
            else:
                # Short wait between checks
                print("No unanalyzed URLs available, waiting 5 seconds...")
                sys.stdout.flush()
                time.sleep(5)
            continue
            
        # Reset pending count when we find a URL
        pending_count = 0
        
        # Get the URL
        url_data = urls[0]
        url = url_data['url']
        print(f"Analyzing URL: {url}")
        sys.stdout.flush()
        
        # Fetch the content from the database
        content = db.get_page_content(url)
        if not content:
            print(f"Error: No content found for URL: {url}")
            sys.stdout.flush()
            time.sleep(1)
            continue
            
        # Analyze the page
        result = analyzer.analyze_page(url, content.get('title', ''), content.get('content', ''), content.get('metadata', ''))
        
        # Add analysis result to PostgreSQL database if it's a manufacturer page
        if result.get('is_manufacturer', False) or result.get('page_type') in ['brand_page', 'brand_category_page']:
            manufacturer_name = result.get('manufacturer_name', '')
            categories = result.get('categories', [])
            
            # Save to PostgreSQL
            if manufacturer_name:
                db.save_manufacturer(manufacturer_name, url, categories)
                print(f"Saved manufacturer: {manufacturer_name} with {len(categories)} categories")
            else:
                print(f"No manufacturer name extracted for URL: {url}")
        else:
            print(f"Not a manufacturer page: {url}")
            
        # Signal that URL is analyzed (will be added to batch for SQLite update)
        result_output = {
            "url": url,
            "status": "analyzed"
        }
        print(f"RESULT: {json.dumps(result_output)}")
        sys.stdout.flush()
        
        # Sleep briefly to avoid overloading the system
        time.sleep(2)
        
    except KeyboardInterrupt:
        print("Analyzer stopped by user")
        break
    except Exception as e:
        print(f"Error in analyzer: {str(e)}")
        sys.stdout.flush()
        time.sleep(5)  # Wait before retrying

# Cleanup
db.close()
print("Analyzer finished")
`

	// Create a temporary file for the script
	tmpfile, err := os.CreateTemp("", "analyzer_*.py")
	if err != nil {
		return fmt.Errorf("failed to create temporary analyzer script file: %w", err)
	}

	// Write the script to the file
	_, err = tmpfile.WriteString(script)
	if err != nil {
		os.Remove(tmpfile.Name())
		return fmt.Errorf("failed to write to temporary analyzer script file: %w", err)
	}

	// Close the file
	err = tmpfile.Close()
	if err != nil {
		os.Remove(tmpfile.Name())
		return fmt.Errorf("failed to close temporary analyzer script file: %w", err)
	}

	// Create the analyzer command
	cmd := exec.Command("python3", tmpfile.Name())
	cmd.Dir = "/var/ndaivimanuales"

	// Set up pipes for stdin/stdout/stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		os.Remove(tmpfile.Name())
		return fmt.Errorf("failed to create stdin pipe for analyzer: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		os.Remove(tmpfile.Name())
		return fmt.Errorf("failed to create stdout pipe for analyzer: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		os.Remove(tmpfile.Name())
		return fmt.Errorf("failed to create stderr pipe for analyzer: %w", err)
	}

	// Start the analyzer process
	analyzerProcess.cmd = cmd
	err = cmd.Start()
	if err != nil {
		os.Remove(tmpfile.Name())
		return fmt.Errorf("failed to start analyzer process: %w", err)
	}

	// Mark the analyzer as running
	analyzerProcess.running = true

	// Set the analyzer process
	c.analyzerProcess = analyzerProcess

	// Handle process exit
	go func() {
		// Wait for the process to exit
		output := "Process exited without error"
		err := cmd.Wait()
		if err != nil {
			output = fmt.Sprintf("Process exited with error: %v", err)
		}

		// Mark the analyzer as not running
		c.analyzerProcess.mutex.Lock()
		c.analyzerProcess.running = false
		c.analyzerProcess.mutex.Unlock()

		// Clean up
		os.Remove(tmpfile.Name())
		stdin.Close() // Close stdin to signal EOF
		stdout.Close() // Close stdout to prevent blocking
		stderr.Close() // Close stderr to prevent blocking
		log.Printf("Analyzer process closed: %s", output)
	}()

	// Process stdout line by line for the results
	// Now use batch processing for analyzed URLs
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			log.Printf("Analyzer output: %s", line)

			// Check if the line contains a result
			if strings.Contains(line, "RESULT:") {
				// Extract the JSON result
				jsonStr := strings.TrimPrefix(line, "RESULT: ")
				
				// Parse the JSON result
				var analysisResult struct {
					URL    string `json:"url"`
					Status string `json:"status"`
				}
				
				err := json.Unmarshal([]byte(jsonStr), &analysisResult)
				if err != nil {
					log.Printf("Error parsing analyzer result: %v", err)
					continue
				}
				
				// Check if the analysis was successful
				if analysisResult.Status == "analyzed" {
					log.Printf("Successfully analyzed URL: %s", analysisResult.URL)
					
					// Add the URL to the batch of analyzed URLs
					if err := c.addUrlToBatch(analysisResult.URL); err != nil {
						log.Printf("Error adding URL to batch: %v", err)
					}

					// Increment processed count
					c.stats.mutex.Lock()
					c.stats.urlsProcessed++
					c.stats.lastUpdated = time.Now()
					c.stats.mutex.Unlock()
				}
			}
		}
		
		if err := scanner.Err(); err != nil {
			log.Printf("Error reading analyzer output: %v", err)
		}
	}()

	// Process stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf("Analyzer error: %s", scanner.Text())
		}
		
		if err := scanner.Err(); err != nil {
			log.Printf("Error reading analyzer stderr: %v", err)
		}
	}()

	log.Println("Analyzer process started")
	return nil
}

// stopAnalyzer stops the analyzer process
func (c *Coordinator) stopAnalyzer() {
	log.Println("Stopping analyzer process")

	if c.analyzerProcess == nil {
		log.Println("Analyzer is not running")
		return
	}

	c.analyzerProcess.mutex.Lock()
	if c.analyzerProcess.running {
		if c.analyzerProcess.cmd.Process != nil {
			err := c.analyzerProcess.cmd.Process.Kill()
			if err != nil {
				log.Printf("Failed to kill analyzer process: %v", err)
			}
		}
		c.analyzerProcess.running = false
	}
	c.analyzerProcess.mutex.Unlock()

	// Process any remaining URLs in the batch
	c.analyzerBatch.mutex.Lock()
	batchSize := len(c.analyzerBatch.urls)
	c.analyzerBatch.mutex.Unlock()
	
	if batchSize > 0 {
		log.Printf("Processing %d remaining URLs in batch before stopping", batchSize)
		if err := c.processBatch(); err != nil {
			log.Printf("Error processing final batch: %v", err)
		}
	}
}

// stopComponents stops the Python components
func (c *Coordinator) stopComponents() {
	// Stop crawler
	if c.crawlerProcess != nil && c.crawlerProcess.running {
		c.stopCrawler()
	}

	// Stop analyzer
	if c.analyzerProcess != nil && c.analyzerProcess.running {
		c.stopAnalyzer()
	}
}

// startCrawler starts the crawler process
func (c *Coordinator) startCrawler() error {
	log.Println("Starting crawler process")

	// Reset statistics when starting a new crawler process
	c.stats.mutex.Lock()
	c.stats.urlsProcessed = 0
	c.stats.urlsQueued = 0
	c.stats.urlsFailed = 0
	c.stats.lastUpdated = time.Now()
	c.stats.mutex.Unlock()
	log.Println("Statistics reset for new crawler process")

	// Get max unanalyzed URLs from config
	maxUnanalyzedUrls := c.config.WebCrawler.MaxUnanalyzedUrls
	if maxUnanalyzedUrls == 0 {
		maxUnanalyzedUrls = 1024  // Default target size
	}

	// Calculate usage percentage
	currentQueued := c.stats.urlsQueued
	_ = float64(currentQueued) / float64(maxUnanalyzedUrls) * 100.0 // Usage calculation for future use

	// Get target website from config
	targetWebsite := c.config.WebCrawler.TargetWebsite
	if targetWebsite == "" {
		targetWebsite = "https://manualslib.com"
		log.Printf("No target website specified in config, using default: %s", targetWebsite)
	} else if !strings.HasPrefix(targetWebsite, "http://") && !strings.HasPrefix(targetWebsite, "https://") {
		// Add https:// prefix if missing
		targetWebsite = "https://" + targetWebsite
		log.Printf("Added https:// prefix to target website: %s", targetWebsite)
	}

	// Create command with the target URL and necessary arguments
	// Set max URLs to a very large number (1000000) to run for a long time and fill up the backlog
	// The crawler should use the allowed_domains from the config.yaml file
	log.Printf("Starting crawler with target URL: %s and max URLs set to 1000000", targetWebsite)
	cmd := exec.Command("python3", "/var/ndaivimanuales/scraper/run_crawler.py", "crawl", targetWebsite, "--max-urls", "1000000")

	// Set the working directory to the project root
	cmd.Dir = "/var/ndaivimanuales"

	// Set environment variables
	cmd.Env = os.Environ()

	// Set up pipes for stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe for crawler: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe for crawler: %w", err)
	}

	// Set process
	c.crawlerProcess = &Process{
		cmd:     cmd,
		name:    "crawler",
		running: true,
	}

	// Start process
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start crawler process: %w", err)
	}

	// Start goroutine to read stdout
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			text := scanner.Text()

			// Direct log handling without intermediate steps to avoid categorization issues
			
			// IMPORTANT: We need to handle the log message differently based on content
			// to ensure proper categorization and stats updating

			// First, check for stats-related messages and update our internal stats
			if strings.Contains(text, "URLs Processed:") {
				// Extract the processed count
				parts := strings.Split(text, "URLs Processed:")
				if len(parts) > 1 {
					processedStr := strings.TrimSpace(strings.Split(parts[1], "\n")[0])
					processed, err := strconv.Atoi(processedStr)
					if err == nil {
						c.stats.mutex.Lock()
						c.stats.urlsProcessed = processed
						c.stats.mutex.Unlock()
						log.Printf("STATS UPDATE: processed=%d", processed)
					}
				}
				// Log as stats
				log.Printf("Crawler Stats: %s", text)
			} else if strings.Contains(text, "URLs Queued:") {
				// Extract the queued count
				parts := strings.Split(text, "URLs Queued:")
				if len(parts) > 1 {
					queuedStr := strings.TrimSpace(strings.Split(parts[1], "\n")[0])
					queued, err := strconv.Atoi(queuedStr)
					if err == nil {
						c.stats.mutex.Lock()
						c.stats.urlsQueued = queued
						c.stats.mutex.Unlock()
						log.Printf("STATS UPDATE: queued=%d", queued)

						// Check if we need to pause the crawler based on queue size
						if queued >= maxUnanalyzedUrls {
							log.Printf("SPEED CONTROL: Queue reached max size (%d >= %d), SLOWING DOWN CRAWLER", 
								queued, maxUnanalyzedUrls)
							// Set crawler to slow mode
							c.setCrawlerSpeed("slow")
							if err != nil {
								log.Printf("Error pausing crawler: %v", err)
							}
						}
					}
				}
				// Log as stats
				log.Printf("Crawler Stats: %s", text)
			} else if strings.Contains(text, "URLs Failed:") {
				// Extract the failed count
				parts := strings.Split(text, "URLs Failed:")
				if len(parts) > 1 {
					failedStr := strings.TrimSpace(strings.Split(parts[1], "\n")[0])
					failed, err := strconv.Atoi(failedStr)
					if err == nil {
						c.stats.mutex.Lock()
						c.stats.urlsFailed = failed
						c.stats.mutex.Unlock()
						log.Printf("STATS UPDATE: failed=%d", failed)
					}
				}
				// Log as stats
				log.Printf("Crawler Stats: %s", text)
			} else if strings.Contains(text, "Stats Update") || strings.Contains(text, "Average Processing Time") {
				// Log as stats
				log.Printf("Crawler Stats: %s", text)
			} else if strings.Contains(text, " - ERROR - ") {
				// Log as error
				log.Printf("Crawler Error: %s", text)
			} else if strings.Contains(text, " - WARNING - ") {
				// Log as warning
				log.Printf("Crawler Warning: %s", text)
			} else if strings.Contains(text, " - INFO - ") {
				// Handle INFO messages
				if strings.Contains(text, "Processing URL:") {
					// Only log occasional URL processing messages to reduce noise
					c.stats.mutex.Lock()
					processed := c.stats.urlsProcessed
					c.stats.mutex.Unlock()
					if processed % 10 == 0 {
						log.Printf("Crawler Info: %s", text)
					}
				} else {
					// Log other INFO messages
					log.Printf("Crawler Info: %s", text)
				}
			} else if strings.Contains(text, " - DEBUG - ") {
				// Only log debug messages if verbose logging is enabled
				if c.config.WebCrawler.VerboseLogging {
					log.Printf("Crawler Debug: %s", text)
				}
			} else if strings.Contains(text, "Crawler started") || 
			          strings.Contains(text, "Crawler stopped") || 
			          strings.Contains(text, "Crawler paused") || 
			          strings.Contains(text, "Crawler resumed") {
				// Log crawler state changes
				log.Printf("Crawler State: %s", text)
			} else {
				// Default for unrecognized messages
				log.Printf("Crawler Message: %s", text)
			}
		}
	}()

	// Start goroutine to read stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			// Only log actual errors from stderr
			text := scanner.Text()
			if strings.Contains(text, "ERROR") || strings.Contains(text, "CRITICAL") {
				log.Printf("Crawler Critical Error: %s", text)
			} else if strings.Contains(text, "WARNING") {
				log.Printf("Crawler Warning: %s", text)
			} else {
				// Only log if it's not an INFO message or if it's an important INFO message
				if !strings.Contains(text, "INFO") || 
				   strings.Contains(text, "Crawler started") || 
				   strings.Contains(text, "Crawler stopped") || 
				   strings.Contains(text, "Crawler paused") || 
				   strings.Contains(text, "Crawler resumed") {
					log.Printf("Crawler Debug: %s", text)
				}
			}
		}
	}()

	// Start goroutine to wait for process
	go func() {
		err := cmd.Wait()
		if err != nil {
			log.Printf("Crawler process exited with error: %v", err)
		} else {
			log.Println("Crawler process exited")
		}

		// Set running flag
		c.crawlerProcess.mutex.Lock()
		c.crawlerProcess.running = false
		c.crawlerProcess.mutex.Unlock()

		// Restart crawler after a delay if it exits
		if c.running {
			log.Println("Restarting crawler after delay...")
			time.Sleep(5 * time.Second)
			err := c.startCrawler()
			if err != nil {
				log.Printf("Failed to restart crawler: %v", err)
			}
		}
	}()

	return nil
}

// stopCrawler stops the crawler process
func (c *Coordinator) stopCrawler() {
	log.Println("Stopping crawler process")

	// Kill process if running
	if c.crawlerProcess != nil && c.crawlerProcess.running {
		c.crawlerProcess.mutex.Lock()
		if c.crawlerProcess.running {
			if c.crawlerProcess.cmd.Process != nil {
				err := c.crawlerProcess.cmd.Process.Kill()
				if err != nil {
					log.Printf("Failed to kill crawler process: %v", err)
				}
			}
			c.crawlerProcess.running = false
		}
		c.crawlerProcess.mutex.Unlock()
	}
}

func (c *Coordinator) getDatabaseStats() (int, int, int, error) {
	// Get the database path from config
	dbPath := c.config.WebCrawler.DBPath
	if dbPath == "" {
		dbPath = "/var/ndaivimanuales/scraper/data/crawler.db"
	}

	// Make sure the database directory exists
	dbDir := filepath.Dir(dbPath)
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		if err := os.MkdirAll(dbDir, 0755); err != nil {
			log.Printf("Warning: Failed to create database directory %s: %v", dbDir, err)
		}
	}

	// Get memory stats as fallback
	c.stats.mutex.Lock()
	memProcessed := c.stats.urlsProcessed
	memQueued := c.stats.urlsQueued
	c.stats.mutex.Unlock()
	memTotal := memProcessed + memQueued

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		// If we have memory stats, use them
		if memTotal > 0 {
			log.Printf("Database file does not exist yet, using memory stats: processed=%d, queued=%d, total=%d",
				memProcessed, memQueued, memTotal)
			return 0, memTotal, 0, nil
		}
		// No database and no memory stats
		return 0, 0, 0, fmt.Errorf("crawler database not yet created")
	}

	// Check if database file exists but is empty or incomplete (still being created)
	fileInfo, err := os.Stat(dbPath)
	if err == nil && fileInfo.Size() < 1024 { // Less than 1KB is likely incomplete
		// If we have memory stats, use them
		if memTotal > 0 {
			log.Printf("Database file exists but appears empty or incomplete, using memory stats")
			return 0, memTotal, 0, nil
		}
		// No valid database and no memory stats
		return 0, 0, 0, fmt.Errorf("crawler database not fully initialized")
	}

	// Try up to 3 times to connect to the database
	var readOnlyDB *database.CrawlerDB
	var dbOpenErr error
	for i := 0; i < 3; i++ {
		// Open a read-only connection to the database
		readOnlyDB, dbOpenErr = database.NewReadOnlyCrawlerDB(dbPath)
		if dbOpenErr == nil {
			break
		}
		log.Printf("Warning: Failed to open read-only database connection (attempt %d/3): %v", i+1, dbOpenErr)
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
	}

	// If all attempts failed, fall back to memory stats
	if dbOpenErr != nil {
		// If we have memory stats, use them
		if memTotal > 0 {
			log.Printf("Warning: All attempts to open database failed, using memory stats")
			return 0, memTotal, 0, nil
		}
		// No valid connection and no memory stats
		return 0, 0, 0, fmt.Errorf("failed to open crawler database: %w", dbOpenErr)
	}

	// Successfully connected to database
	defer readOnlyDB.Close()

	// First, check if the pages table exists
	var tableExists bool
	tableCheckErr := readOnlyDB.DB().QueryRow(
		"SELECT name FROM sqlite_master WHERE type='table' AND name='pages'").Scan(&tableExists)
	
	if tableCheckErr != nil || !tableExists {
		// Table doesn't exist yet, probably still initializing
		if memTotal > 0 {
			log.Printf("Database exists but 'pages' table not found, using memory stats")
			return 0, memTotal, 0, nil
		}
		return 0, 0, 0, fmt.Errorf("crawler database tables not initialized")
	}

	// Get stats from the read-only database with retries
	var stats database.CrawlerStats
	var statsErr error
	for i := 0; i < 3; i++ {
		stats, statsErr = readOnlyDB.GetStats()
		if statsErr == nil {
			break
		}
		log.Printf("Warning: Failed to get stats from database (attempt %d/3): %v", i+1, statsErr)
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
	}

	// If all attempts to get stats failed, fall back to memory stats
	if statsErr != nil {
		// If we have memory stats, use them
		if memTotal > 0 {
			log.Printf("Warning: All attempts to get database stats failed, using memory stats")
			return 0, memTotal, 0, nil
		}
		// No valid stats and no memory stats
		return 0, 0, 0, fmt.Errorf("failed to get crawler stats: %w", statsErr)
	}

	// Successfully got stats from database
	log.Printf("Database stats: analyzed=%d, total=%d, pending=%d", 
		stats.AnalyzedURLs, stats.TotalURLs, stats.TotalURLs - stats.AnalyzedURLs)
	
	// Get current batch size
	c.analyzerBatch.mutex.Lock()
	batchSize := len(c.analyzerBatch.urls)
	c.analyzerBatch.mutex.Unlock()
	
	return stats.AnalyzedURLs, stats.TotalURLs, batchSize, nil
}

// manageCrawlSpeed continuously monitors the number of unanalyzed URLs and adjusts crawler speed
func (c *Coordinator) manageCrawlSpeed() {
	log.Println("Starting dynamic crawler speed control")
	
	speedCheckInterval := c.config.WebCrawler.CrawlSpeedCheckInterval
	if speedCheckInterval <= 0 {
		speedCheckInterval = 5 // Default: check every 5 seconds
	}
	
	maxUnanalyzedUrls := c.config.WebCrawler.MaxUnanalyzedUrls
	if maxUnanalyzedUrls <= 0 {
		maxUnanalyzedUrls = 1024 // Default: slow down at 1024 unanalyzed URLs
	}
	
	// Calculate the threshold for resuming normal speed (75% of max)
	normalSpeedThreshold := int(float64(maxUnanalyzedUrls) * 0.75)
	
	for {
		select {
		case <-c.stopCh:
			log.Println("Stopping crawler speed control")
			return
		case <-time.After(time.Duration(speedCheckInterval) * time.Second):
			// Get current stats
			analyzedUrls, totalUrls, _, err := c.getDatabaseStats()
			if err != nil {
				log.Printf("Warning: Failed to get database stats for speed control: %v", err)
				continue
			}
			
			// Calculate unanalyzed URLs
			unanalyzedUrls := totalUrls - analyzedUrls
			
			// Get current speed mode
			c.crawlerStatus.mutex.Lock()
			currentSpeedMode := c.crawlerStatus.speedMode
			c.crawlerStatus.mutex.Unlock()
			
			// Adjust speed if needed
			if unanalyzedUrls > maxUnanalyzedUrls && currentSpeedMode != "slow" {
				log.Printf("Unanalyzed URLs (%d) exceeds threshold (%d), slowing down crawler", 
					unanalyzedUrls, maxUnanalyzedUrls)
				err := c.setCrawlerSpeed("slow")
				if err != nil {
					log.Printf("Warning: Failed to slow down crawler: %v", err)
				}
			} else if unanalyzedUrls < normalSpeedThreshold && currentSpeedMode != "normal" {
				log.Printf("Unanalyzed URLs (%d) below threshold (%d), resuming normal crawler speed", 
					unanalyzedUrls, normalSpeedThreshold)
				err := c.setCrawlerSpeed("normal")
				if err != nil {
					log.Printf("Warning: Failed to resume normal crawler speed: %v", err)
				}
			}
		}
	}
}

// collectStats collects statistics about the system
func (c *Coordinator) collectStats() {
	// Collect stats every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Save stats
			c.saveStats()
		case <-c.stopCh:
			return
		}
	}
}

// saveStats saves statistics to the database
func (c *Coordinator) saveStats() {
	// Get URL counts from the database
	analyzedUrls, totalUrls, batchSize, err := c.getDatabaseStats()
	if err != nil {
		log.Printf("Error getting database stats: %v", err)
		return
	}

	// Get memory stats for full reporting
	c.stats.mutex.Lock()
	memProcessed := c.stats.urlsProcessed
	memQueued := c.stats.urlsQueued
	memFailed := c.stats.urlsFailed
	memTotal := memProcessed + memQueued
	c.stats.mutex.Unlock()

	// Use memTotal in log statement to avoid unused variable warning
	log.Printf("Memory stats: processed=%d, queued=%d, failed=%d, total=%d", 
		memProcessed, memQueued, memFailed, memTotal)

	// Create stats map
	stats := make(map[string]interface{})

	// Add crawler stats to the database
	stats["urls_analyzed"] = analyzedUrls
	stats["urls_total"] = totalUrls
	stats["urls_pending"] = totalUrls - analyzedUrls
	stats["urls_batch_size"] = batchSize
	stats["urls_batch_max"] = c.analyzerBatch.maxBatchSize
	stats["urls_failed"] = memFailed

	// Get current crawler speed mode
	c.crawlerStatus.mutex.Lock()
	speedMode := c.crawlerStatus.speedMode
	stats["crawler_speed"] = speedMode
	c.crawlerStatus.mutex.Unlock()

	// Add database stats if available
	if c.db != nil {
		manufacturers, err := c.db.GetManufacturers()
		if err != nil {
			log.Printf("Error getting manufacturers: %v", err)
		} else {
			stats["manufacturers_count"] = len(manufacturers)
		}
	} else {
		stats["manufacturers_count"] = 0
	}

	// Marshal stats to JSON
	statsJSON, err := json.Marshal(stats)
	if err != nil {
		log.Printf("Error marshaling stats: %v", err)
		return
	}

	// Save stats to database if available
	if c.db != nil {
		err = c.db.SaveStats("system", string(statsJSON))
		if err != nil {
			log.Printf("Error saving stats: %v", err)
		}
	}

	// Log status and statistics including batch progress
	log.Printf("STATUS UPDATE: %d URLs analyzed, %d URLs in database (%d pending analysis), %d URLs in current batch (%d/%d), %d failed URLs, crawler speed: %s",
		analyzedUrls,               // Analyzed URLs in database
		totalUrls,                 // Total URLs in database
		totalUrls - analyzedUrls,  // Pending URLs (not yet analyzed)
		batchSize,                 // URLs in current batch
		batchSize,                 // Current batch size
		c.analyzerBatch.maxBatchSize, // Max batch size
		memFailed,                 // Failed URLs
		speedMode)                 // Current crawler speed
}

// pauseCrawler temporarily stops the crawler process
func (c *Coordinator) pauseCrawler() error {
	c.crawlerProcess.mutex.Lock()
	defer c.crawlerProcess.mutex.Unlock()

	if c.crawlerProcess == nil || !c.crawlerProcess.running {
		log.Println("Crawler is not running, nothing to pause")
		return nil
	}

	// Check if already paused to avoid duplicate signals
	if c.crawlerProcess.paused {
		log.Println("Crawler is already paused")
		return nil
	}

	log.Println("Pausing crawler process")

	// Send SIGSTOP to pause the process
	err := c.crawlerProcess.cmd.Process.Signal(syscall.SIGSTOP)
	if err != nil {
		return fmt.Errorf("failed to pause crawler process: %w", err)
	}

	// Mark as paused
	c.crawlerProcess.paused = true
	log.Println("Crawler process paused")
	return nil
}

// resumeCrawler resumes the paused crawler process
func (c *Coordinator) resumeCrawler() error {
	c.crawlerProcess.mutex.Lock()
	defer c.crawlerProcess.mutex.Unlock()

	if c.crawlerProcess == nil || !c.crawlerProcess.running {
		log.Println("Crawler is not running, nothing to resume")
		return nil
	}

	// Check if already running to avoid duplicate signals
	if !c.crawlerProcess.paused {
		log.Println("Crawler is already running")
		return nil
	}

	log.Println("Resuming crawler process")

	// Send SIGCONT to resume the process
	err := c.crawlerProcess.cmd.Process.Signal(syscall.SIGCONT)
	if err != nil {
		return fmt.Errorf("failed to resume crawler process: %w", err)
	}

	// Mark as not paused
	c.crawlerProcess.paused = false
	log.Println("Crawler process resumed")
	return nil
}

// addUrlToBatch adds a URL to the batch of analyzed URLs
// When the batch reaches the maximum size, it processes the batch
func (c *Coordinator) addUrlToBatch(url string) error {
	// Add URL to the batch
	c.analyzerBatch.mutex.Lock()
	defer c.analyzerBatch.mutex.Unlock()
	
	// Check if URL is already in the batch
	for _, existingUrl := range c.analyzerBatch.urls {
		if existingUrl == url {
			log.Printf("URL already in batch, skipping: %s", url)
			return nil
		}
	}
	
	// Add URL to batch
	c.analyzerBatch.urls = append(c.analyzerBatch.urls, url)
	log.Printf("Added URL to batch (%d/%d): %s", 
		len(c.analyzerBatch.urls), c.analyzerBatch.maxBatchSize, url)
	
	// Process batch if it has reached maximum size
	if len(c.analyzerBatch.urls) >= c.analyzerBatch.maxBatchSize {
		log.Printf("Batch full (%d URLs), processing...", len(c.analyzerBatch.urls))
		return c.processBatch()
	}
	
	return nil
}

// processBatch processes the current batch of analyzed URLs
// This involves pausing the crawler, marking all URLs as analyzed, and then resuming
func (c *Coordinator) processBatch() error {
	// Lock the batch to prevent changes during processing
	c.analyzerBatch.mutex.Lock()
	defer c.analyzerBatch.mutex.Unlock()
	
	// Check if batch is empty
	if len(c.analyzerBatch.urls) == 0 {
		log.Println("Batch is empty, nothing to process")
		return nil
	}
	
	// Stop the crawler before updating the database
	log.Println("Pausing crawler to update analyzed URLs in database")
	err := c.pauseCrawler()
	if err != nil {
		return fmt.Errorf("failed to pause crawler before processing batch: %w", err)
	}
	
	// Create a temporary file with the batch URLs
	log.Printf("Writing %d analyzed URLs to temp file: %s", 
		len(c.analyzerBatch.urls), c.analyzerBatch.batchFilePath)
	
	// Marshal the URLs to JSON
	urlsJson, err := json.Marshal(c.analyzerBatch.urls)
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to marshal URLs to JSON: %w", err)
	}
	
	// Create the batch file directory if it doesn't exist
	batchDir := filepath.Dir(c.analyzerBatch.batchFilePath)
	if _, err := os.Stat(batchDir); os.IsNotExist(err) {
		if err := os.MkdirAll(batchDir, 0755); err != nil {
			// Resume crawler before returning error
			c.resumeCrawler()
			return fmt.Errorf("failed to create batch directory: %w", err)
		}
	}
	
	// Write the JSON to the temp file
	err = os.WriteFile(c.analyzerBatch.batchFilePath, urlsJson, 0644)
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to write URLs to temp file: %w", err)
	}
	
	// Create a Python script to mark all URLs as analyzed
	script := fmt.Sprintf(`
import sys
import json
sys.path.insert(0, '/var/ndaivimanuales')
from scraper.db_manager import DBManager

db = DBManager()
try:
    # Load URLs from the batch file
    with open('%s', 'r') as f:
        urls = json.load(f)
    
    # Mark each URL as analyzed
    success_count = 0
    for url in urls:
        db.mark_as_analyzed(url)
        success_count += 1
    
    print(f"Successfully marked {success_count}/{len(urls)} URLs as analyzed")
except Exception as e:
    print(f"Error: {str(e)}")
    sys.exit(1)
finally:
    db.close()
`, c.analyzerBatch.batchFilePath)
	
	// Create a temporary file for the script
	tmpfile, err := os.CreateTemp("", "mark_analyzed_*.py")
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to create temporary script file: %w", err)
	}
	defer os.Remove(tmpfile.Name())
	
	// Write the script to the file
	_, err = tmpfile.WriteString(script)
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to write to temporary script file: %w", err)
	}
	
	// Close the file
	err = tmpfile.Close()
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to close temporary script file: %w", err)
	}
	
	// Execute the Python script
	cmd := exec.Command("python3", tmpfile.Name())
	cmd.Dir = "/var/ndaivimanuales"
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to execute Python script: %w, output: %s", err, string(output))
	}
	
	// Log the script output
	log.Printf("Batch processing output: %s", string(output))
	
	// Clear the batch
	c.analyzerBatch.urls = []string{}
	
	// Resume the crawler
	log.Println("Batch processing complete, resuming crawler")
	err = c.resumeCrawler()
	if err != nil {
		return fmt.Errorf("failed to resume crawler after processing batch: %w", err)
	}
	
	return nil
}

// markUrlAsAnalyzed marks a URL as analyzed in the crawler database
// This is now only used for individual URLs, not for batch processing
func (c *Coordinator) markUrlAsAnalyzed(url string) error {
	log.Printf("Marking URL as analyzed: %s", url)

	// Pause the crawler to avoid database locking issues
	err := c.pauseCrawler()
	if err != nil {
		log.Printf("Warning: Failed to pause crawler: %v", err)
	}

	// Create a Python script to mark the URL as analyzed
	script := fmt.Sprintf(`
import sys
sys.path.insert(0, '/var/ndaivimanuales')
from scraper.db_manager import DBManager

db = DBManager()
success = db.mark_url_as_analyzed('%s')
db.close()
print('success' if success else 'failure')
`, url)

	// Create a temporary file for the script
	tmpfile, err := os.CreateTemp("", "mark_analyzed_*.py")
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to create temporary script file: %w", err)
	}
	defer os.Remove(tmpfile.Name())

	// Write the script to the file
	_, err = tmpfile.WriteString(script)
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to write to temporary script file: %w", err)
	}

	// Close the file
	err = tmpfile.Close()
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to close temporary script file: %w", err)
	}

	// Execute the Python script
	cmd := exec.Command("python3", tmpfile.Name())
	cmd.Dir = "/var/ndaivimanuales"
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to execute Python script: %w, output: %s", err, string(output))
	}

	// Check if the operation was successful
	if strings.TrimSpace(string(output)) != "success" {
		// Resume crawler before returning error
		c.resumeCrawler()
		return fmt.Errorf("failed to mark URL as analyzed: %s", string(output))
	}

	log.Printf("Successfully marked URL as analyzed: %s", url)

	// Resume the crawler
	err = c.resumeCrawler()
	if err != nil {
		return fmt.Errorf("failed to resume crawler after marking URL as analyzed: %w", err)
	}

	return nil
}

// setCrawlerSpeed sets the crawler speed to either 'normal' or 'slow'
func (c *Coordinator) setCrawlerSpeed(speed string) error {
	if c.crawlerProcess == nil || !c.crawlerProcess.running {
		return fmt.Errorf("crawler process not running")
	}

	// Lock the crawler process
	c.crawlerProcess.mutex.Lock()
	defer c.crawlerProcess.mutex.Unlock()

	// Set the appropriate delay based on the speed
	delay := "0"  // No delay for normal speed
	if speed == "slow" {
		// Use a delay of 2 seconds for slow mode
		delay = "2"
		c.crawlerStatus.mutex.Lock()
		c.crawlerStatus.speedMode = "slow"
		c.crawlerStatus.lastUpdated = time.Now()
		c.crawlerStatus.mutex.Unlock()
		log.Println("Setting crawler to slow mode with 2 second delay")
	} else {
		c.crawlerStatus.mutex.Lock()
		c.crawlerStatus.speedMode = "normal"
		c.crawlerStatus.lastUpdated = time.Now()
		c.crawlerStatus.mutex.Unlock()
		log.Println("Setting crawler to normal speed with no delay")
	}

	// Send the speed command to the crawler
	// Format: SPEED:<delay_seconds>
	command := fmt.Sprintf("SPEED:%s\n", delay)
	stdin, ok := c.crawlerProcess.cmd.Stdin.(*os.File)
	if !ok {
		return fmt.Errorf("crawler process stdin is not a file")
	}
	_, err := stdin.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send speed command to crawler: %w", err)
	}

	return nil
}

// getUnanalyzedUrls gets a list of URLs that have been visited but not analyzed
func (c *Coordinator) getUnanalyzedUrls(limit int) ([]string, error) {
	log.Printf("Getting up to %d unanalyzed URLs", limit)

	// Pause the crawler to avoid database locking issues
	err := c.pauseCrawler()
	if err != nil {
		log.Printf("Warning: Failed to pause crawler: %v", err)
	}
	defer c.resumeCrawler()

	// Create a Python script to get unanalyzed URLs
	script := fmt.Sprintf(`
import sys
import json
sys.path.insert(0, '/var/ndaivimanuales')
from scraper.db_manager import DBManager

db = DBManager()
urls = db.get_unanalyzed_urls(%d)
db.close()
print(json.dumps([url['url'] for url in urls]))
`, limit)

	// Create a temporary file for the script
	tmpfile, err := os.CreateTemp("", "get_unanalyzed_*.py")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary script file: %w", err)
	}
	defer os.Remove(tmpfile.Name())

	// Write the script to the file
	_, err = tmpfile.WriteString(script)
	if err != nil {
		return nil, fmt.Errorf("failed to write to temporary script file: %w", err)
	}

	// Close the file
	err = tmpfile.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close temporary script file: %w", err)
	}

	// Execute the Python script
	cmd := exec.Command("python3", tmpfile.Name())
	cmd.Dir = "/var/ndaivimanuales"
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Python script: %w, output: %s", err, string(output))
	}

	// Parse the output as JSON
	var urls []string
	err = json.Unmarshal(output, &urls)
	if err != nil {
		return nil, fmt.Errorf("failed to parse output as JSON: %w, output: %s", err, string(output))
	}

	log.Printf("Got %d unanalyzed URLs", len(urls))
	return urls, nil
}
