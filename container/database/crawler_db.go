package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// CrawlerDB represents a connection to the crawler SQLite database
type CrawlerDB struct {
	db    *sql.DB
	mutex sync.Mutex
}

// CrawlerStats contains statistics about the crawler database
type CrawlerStats struct {
	TotalURLs    int
	AnalyzedURLs int
}

// NewCrawlerDB creates a new connection to the crawler SQLite database
func NewCrawlerDB(dbPath string) (*CrawlerDB, error) {
	// Open SQLite database with WAL mode and busy timeout
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s?_journal=WAL&_timeout=5000&_busy_timeout=5000", dbPath))
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	// Enable foreign keys
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error enabling foreign keys: %v", err)
	}

	// Set busy timeout
	_, err = db.Exec("PRAGMA busy_timeout = 5000")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error setting busy timeout: %v", err)
	}

	// Set journal mode to WAL
	_, err = db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error setting journal mode: %v", err)
	}

	// Set synchronous mode to NORMAL for better performance
	_, err = db.Exec("PRAGMA synchronous = NORMAL")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error setting synchronous mode: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(1) // SQLite only supports one writer at a time
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &CrawlerDB{db: db}, nil
}

// NewReadOnlyCrawlerDB creates a read-only connection to the crawler SQLite database
func NewReadOnlyCrawlerDB(dbPath string) (*CrawlerDB, error) {
	// Open SQLite database in read-only mode with improved settings
	// Using URI connection string with immutable flag to ensure read-only
	connStr := fmt.Sprintf("file:%s?mode=ro&immutable=1&_timeout=5000&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database in read-only mode: %v", err)
	}

	// Configure connection for optimal read-only performance
	_, err = db.Exec("PRAGMA query_only = ON")
	if err != nil {
		log.Printf("Warning: Could not set PRAGMA query_only: %v", err)
	}

	_, err = db.Exec("PRAGMA temp_store = MEMORY")
	if err != nil {
		log.Printf("Warning: Could not set PRAGMA temp_store: %v", err)
	}

	_, err = db.Exec("PRAGMA locking_mode = NORMAL")
	if err != nil {
		log.Printf("Warning: Could not set PRAGMA locking_mode: %v", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(2) // Allow just a couple of connections for reading
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(10 * time.Second) // Close connections quickly

	// Test connection with retries
	var pingErr error
	for i := 0; i < 3; i++ { // Try up to 3 times
		pingErr = db.Ping()
		if pingErr == nil {
			break
		}
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
	}

	if pingErr != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping read-only database after retries: %w", pingErr)
	}

	return &CrawlerDB{db: db}, nil
}

// Close closes the database connection
func (c *CrawlerDB) Close() error {
	return c.db.Close()
}

// DB returns the underlying database connection (read-only access)
func (c *CrawlerDB) DB() *sql.DB {
	return c.db
}

// GetStats returns statistics about the crawler database
func (c *CrawlerDB) GetStats() (CrawlerStats, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var stats CrawlerStats

	// Get total URLs count with retry
	for attempt := 0; attempt < 5; attempt++ {
		err := c.db.QueryRow("SELECT COUNT(*) FROM pages").Scan(&stats.TotalURLs)
		if err == nil {
			break
		}

		if attempt == 4 {
			return stats, fmt.Errorf("failed to get total URLs count: %w", err)
		}

		log.Printf("Database busy, retrying in %d ms", 100*(attempt+1))
		time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
	}

	// Get analyzed URLs count with retry
	for attempt := 0; attempt < 5; attempt++ {
		err := c.db.QueryRow("SELECT COUNT(*) FROM pages WHERE analyzed = 1").Scan(&stats.AnalyzedURLs)
		if err == nil {
			break
		}

		if attempt == 4 {
			return stats, fmt.Errorf("failed to get analyzed URLs count: %w", err)
		}

		log.Printf("Database busy, retrying in %d ms", 100*(attempt+1))
		time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
	}

	return stats, nil
}

// URLWithPriority represents a URL with its priority score
type URLWithPriority struct {
	URL      string
	Priority int
}

// CalculateURLPriority calculates a priority score for a URL based on various factors
// Higher priority scores mean the URL should be processed sooner
func CalculateURLPriority(url string) int {
	// Base priority - all URLs start with this
	priority := 100

	// Shorter URLs typically have higher value (manufacturer/category pages)
	// Subtract 1 point for every 10 characters in length
	priority -= len(url) / 10

	// Add priority for URLs containing important keywords
	// These are keywords that suggest the URL leads to valuable content
	keywords := []string{
		"manufacturer", "brand", "company", "companies", "products",
		"category", "catalog", "brands", "manual", "manuals",
		"model", "series", "product-line", "lineup", "range",
	}

	// Add priority for each keyword found
	for _, keyword := range keywords {
		if strings.Contains(strings.ToLower(url), keyword) {
			priority += 15
		}
	}

	// Reduce priority for URLs containing low-value keywords
	lowValueKeywords := []string{
		"login", "register", "cart", "checkout", "account",
		"privacy", "terms", "contact", "about", "faq",
		"support", "help", "news", "blog",
	}

	// Reduce priority for each low-value keyword found
	for _, keyword := range lowValueKeywords {
		if strings.Contains(strings.ToLower(url), keyword) {
			priority -= 20
		}
	}

	// Ensure minimum priority
	if priority < 1 {
		priority = 1
	}

	return priority
}

// GetUnanalyzedURLs returns a list of unanalyzed URLs ordered by priority
func (c *CrawlerDB) GetUnanalyzedURLs(limit int) ([]string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// First check if we have a priority column in the pages table
	var hasPriorityColumn bool
	err := c.db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('pages') WHERE name='priority'").Scan(&hasPriorityColumn)
	if err != nil {
		log.Printf("Warning: Error checking for priority column: %v", err)
		hasPriorityColumn = false
	}

	// Query to use based on whether priority column exists
	var queryStr string
	if hasPriorityColumn {
		// Use priority-based ordering if column exists
		queryStr = `
			SELECT url FROM pages 
			WHERE analyzed = 0 
			ORDER BY priority DESC, depth ASC, length(url) ASC LIMIT ?
		`
	} else {
		// Fall back to standard ordering if priority column doesn't exist
		queryStr = `
			SELECT url FROM pages 
			WHERE analyzed = 0 
			ORDER BY depth ASC, length(url) ASC, crawled_at ASC LIMIT ?
		`
	}

	// Execute the query
	rows, err := c.db.Query(queryStr, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query unanalyzed URLs: %w", err)
	}
	defer rows.Close()

	var urls []string
	for rows.Next() {
		var url string
		err := rows.Scan(&url)
		if err != nil {
			return nil, fmt.Errorf("failed to scan URL: %w", err)
		}
		urls = append(urls, url)
	}

	return urls, nil
}

// MarkURLAsAnalyzed marks a URL as analyzed
func (c *CrawlerDB) MarkURLAsAnalyzed(url string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, err := c.db.Exec("UPDATE pages SET analyzed = 1 WHERE url = ?", url)
	if err != nil {
		return fmt.Errorf("failed to mark URL as analyzed: %w", err)
	}

	return nil
}
