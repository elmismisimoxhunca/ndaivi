package main

import (
	"fmt"
	"log"
	"os"

	"github.com/ndaivi/container/cli"
	"github.com/ndaivi/container/config"
	"github.com/ndaivi/container/database"
	"github.com/ndaivi/container/redis"
)

func main() {
	// Set up logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Default config path
	configPath := "/var/ndaivimanuales/config.yaml"
	if len(os.Args) > 1 && os.Args[1] == "--config" && len(os.Args) > 2 {
		configPath = os.Args[2]
		// Remove the --config and path from args
		os.Args = append(os.Args[:1], os.Args[3:]...)
	}

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize Redis client
	redisClient, err := redis.NewClient(cfg.Redis)
	if err != nil {
		fmt.Printf("Failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	// Set channels in Redis client
	redisClient.SetChannels(cfg.Application.Channels)

	// Initialize PostgreSQL database
	db, err := database.NewPostgresDB(cfg.Database)
	if err != nil {
		fmt.Printf("Failed to connect to PostgreSQL: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Create CLI
	cli := cli.NewCLI(cfg, redisClient, db)

	// Run CLI
	err = cli.Run(os.Args)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
