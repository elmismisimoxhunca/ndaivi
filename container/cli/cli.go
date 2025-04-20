package cli

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ndaivi/container/config"
	"github.com/ndaivi/container/database"
	"github.com/ndaivi/container/redis"
)

// CLI represents the command-line interface
type CLI struct {
	config      *config.Config
	redisClient *redis.Client
	db          *database.PostgresDB
}

// NewCLI creates a new CLI instance
func NewCLI(cfg *config.Config, redisClient *redis.Client, db *database.PostgresDB) *CLI {
	// Set channels in Redis client
	redisClient.SetChannels(cfg.Application.Channels)

	return &CLI{
		config:      cfg,
		redisClient: redisClient,
		db:          db,
	}
}

// Run runs the CLI
func (c *CLI) Run(args []string) error {
	// Check if command is provided
	if len(args) < 2 {
		c.printUsage()
		return nil
	}

	// Parse command
	command := args[1]

	// Execute command
	switch command {
	case "start-crawler":
		return c.startCrawler(args[2:])
	case "stop-crawler":
		return c.stopCrawler()
	case "start-analyzer":
		return c.startAnalyzer()
	case "stop-analyzer":
		return c.stopAnalyzer()
	case "status":
		return c.showStatus()
	case "stats":
		return c.showStats()
	case "manufacturers":
		return c.showManufacturers()
	case "categories":
		return c.showCategories(args[2:])
	case "help":
		c.printUsage()
		return nil
	default:
		fmt.Printf("Unknown command: %s\n", command)
		c.printUsage()
		return nil
	}
}

// printUsage prints the CLI usage
func (c *CLI) printUsage() {
	fmt.Println("NDAIVI Container CLI")
	fmt.Println("Usage: ndaivi-container <command> [arguments]")
	fmt.Println("\nCommands:")
	fmt.Println("  start-crawler [url] [max-urls]  Start the crawler with optional URL and max URLs")
	fmt.Println("  stop-crawler                    Stop the crawler")
	fmt.Println("  start-analyzer                  Start the analyzer")
	fmt.Println("  stop-analyzer                   Stop the analyzer")
	fmt.Println("  status                          Show system status")
	fmt.Println("  stats                           Show system statistics")
	fmt.Println("  manufacturers                   List all manufacturers")
	fmt.Println("  categories <manufacturer-id>    List categories for a manufacturer")
	fmt.Println("  help                            Show this help message")
}

// startCrawler starts the crawler
func (c *CLI) startCrawler(args []string) error {
	// Parse arguments
	startURL := c.config.WebCrawler.TargetWebsite
	maxURLs := 0

	if len(args) > 0 && args[0] != "" {
		startURL = args[0]
	}

	if len(args) > 1 {
		var err error
		maxURLs, err = strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("invalid max URLs: %w", err)
		}
	}

	// Start crawler
	fmt.Printf("Starting crawler with URL %s and max URLs %d\n", startURL, maxURLs)
	return c.redisClient.StartCrawler(startURL, maxURLs)
}

// stopCrawler stops the crawler
func (c *CLI) stopCrawler() error {
	fmt.Println("Stopping crawler")
	return c.redisClient.StopCrawler()
}

// startAnalyzer starts the analyzer
func (c *CLI) startAnalyzer() error {
	fmt.Println("Starting analyzer")
	return c.redisClient.StartAnalyzer()
}

// stopAnalyzer stops the analyzer
func (c *CLI) stopAnalyzer() error {
	fmt.Println("Stopping analyzer")
	return c.redisClient.StopAnalyzer()
}

// showStatus shows the system status
func (c *CLI) showStatus() error {
	// Get backlog status
	backlogStatus, err := c.redisClient.GetBacklogStatus()
	if err != nil {
		return fmt.Errorf("failed to get backlog status: %w", err)
	}

	// Get system stats
	statsJSON, err := c.db.GetStats("system")
	if err != nil {
		return fmt.Errorf("failed to get system stats: %w", err)
	}

	// Parse stats
	var stats map[string]interface{}
	err = json.Unmarshal([]byte(statsJSON), &stats)
	if err != nil {
		return fmt.Errorf("failed to parse system stats: %w", err)
	}

	// Print status
	fmt.Println("NDAIVI System Status")
	fmt.Println("====================")

	// Print backlog status
	fmt.Println("\nBacklog Status:")
	fmt.Printf("  Size: %v\n", backlogStatus["backlog_size"])
	fmt.Printf("  Capacity: %v\n", backlogStatus["backlog_capacity"])
	fmt.Printf("  Usage: %.2f%%\n", backlogStatus["backlog_usage"].(float64)*100)

	// Print crawler status
	fmt.Println("\nCrawler Status:")
	fmt.Printf("  URLs Processed: %v\n", stats["urls_processed"])
	fmt.Printf("  URLs Queued: %v\n", stats["urls_queued"])
	fmt.Printf("  URLs Failed: %v\n", stats["urls_failed"])

	// Print analyzer status
	fmt.Println("\nAnalyzer Status:")
	fmt.Printf("  Manufacturers: %v\n", stats["manufacturers_count"])

	return nil
}

// showStats shows the system statistics
func (c *CLI) showStats() error {
	// Get system stats
	statsJSON, err := c.db.GetStats("system")
	if err != nil {
		return fmt.Errorf("failed to get system stats: %w", err)
	}

	// Parse stats
	var stats map[string]interface{}
	err = json.Unmarshal([]byte(statsJSON), &stats)
	if err != nil {
		return fmt.Errorf("failed to parse system stats: %w", err)
	}

	// Print stats
	fmt.Println("NDAIVI System Statistics")
	fmt.Println("=======================\n")

	// Print all stats
	for key, value := range stats {
		// Format timestamp
		if key == "timestamp" {
			timestamp, ok := value.(float64)
			if ok {
				timeStr := time.Unix(int64(timestamp), 0).Format(time.RFC3339)
				fmt.Printf("%-20s: %s\n", key, timeStr)
				continue
			}
		}

		// Format percentage
		if strings.Contains(key, "usage") {
			usage, ok := value.(float64)
			if ok {
				fmt.Printf("%-20s: %.2f%%\n", key, usage*100)
				continue
			}
		}

		// Print other stats
		fmt.Printf("%-20s: %v\n", key, value)
	}

	return nil
}

// showManufacturers shows all manufacturers
func (c *CLI) showManufacturers() error {
	// Get manufacturers
	manufacturers, err := c.db.GetManufacturers()
	if err != nil {
		return fmt.Errorf("failed to get manufacturers: %w", err)
	}

	// Print manufacturers
	fmt.Println("NDAIVI Manufacturers")
	fmt.Println("====================")
	fmt.Printf("\nTotal: %d manufacturers\n\n", len(manufacturers))

	// Print table header
	fmt.Printf("%-5s | %-50s\n", "ID", "Name")
	fmt.Println(strings.Repeat("-", 60))

	// Print manufacturers
	for _, m := range manufacturers {
		fmt.Printf("%-5d | %-50s\n", m.ID, m.Name)
	}

	return nil
}

// showCategories shows categories for a manufacturer
func (c *CLI) showCategories(args []string) error {
	// Check if manufacturer ID is provided
	if len(args) < 1 {
		return fmt.Errorf("manufacturer ID is required")
	}

	// Parse manufacturer ID
	manufacturerID, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid manufacturer ID: %w", err)
	}

	// Get language
	language := c.config.Languages.Source
	if len(args) > 1 {
		language = args[1]
	}

	// Get categories
	categories, err := c.db.GetCategories(manufacturerID, language)
	if err != nil {
		return fmt.Errorf("failed to get categories: %w", err)
	}

	// Get manufacturer
	manufacturers, err := c.db.GetManufacturers()
	if err != nil {
		return fmt.Errorf("failed to get manufacturers: %w", err)
	}

	// Find manufacturer name
	manufacturerName := fmt.Sprintf("Manufacturer ID %d", manufacturerID)
	for _, m := range manufacturers {
		if m.ID == manufacturerID {
			manufacturerName = m.Name
			break
		}
	}

	// Print categories
	fmt.Printf("Categories for %s (Language: %s)\n", manufacturerName, language)
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("\nTotal: %d categories\n\n", len(categories))

	// Print table header
	fmt.Printf("%-5s | %-50s\n", "ID", "Name")
	fmt.Println(strings.Repeat("-", 60))

	// Print categories
	for _, c := range categories {
		fmt.Printf("%-5d | %-50s\n", c.ID, c.Name)
	}

	return nil
}
