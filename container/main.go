package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ndaivi/container/config"
	"github.com/ndaivi/container/coordinator"
	"github.com/ndaivi/container/database"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "/var/ndaivimanuales/config.yaml", "Path to configuration file")
	daemon := flag.Bool("daemon", false, "Run as a daemon in the background")
	statusCheck := flag.Bool("status", false, "Check the status of the running daemon")
	flag.Parse()

	// Check if status check was requested
	if *statusCheck {
		checkStatus()
		return
	}

	// Run as daemon if requested
	if *daemon {
		runAsDaemon()
		return
	}

	// Initialize logger
	logFile := setupLogging()
	if logFile != nil {
		defer logFile.Close()
	}

	log.Println("Starting NDAIVI Container Application")

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize PostgreSQL database
	db, err := database.NewPostgresDB(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Create coordinator without Redis dependency
	coord := coordinator.NewCoordinator(cfg, db)

	// Start the coordinator
	coord.Run()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigCh
	fmt.Printf("\nReceived signal %v, shutting down gracefully...\n", sig)

	// Stop the coordinator
	coord.Stop()

	log.Println("NDAIVI Container Application stopped")
}

// runAsDaemon starts the application as a daemon process
func runAsDaemon() {
	// Get the path to the current executable
	execPath, err := os.Executable()
	if err != nil {
		fmt.Printf("Error getting executable path: %v\n", err)
		os.Exit(1)
	}

	// Create a new process
	cmd := exec.Command(execPath)

	// Set the working directory
	cmd.Dir = "/var/ndaivimanuales/container"

	// Detach the process
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	// Start the process
	err = cmd.Start()
	if err != nil {
		fmt.Printf("Error starting daemon: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("NDAIVI Container Application started as daemon with PID %d\n", cmd.Process.Pid)

	// Save PID to file
	pidFile := "/var/ndaivimanuales/container/ndaivi.pid"
	err = os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644)
	if err != nil {
		fmt.Printf("Error writing PID file: %v\n", err)
	}

	os.Exit(0)
}

// checkStatus checks the status of the running daemon
func checkStatus() {
	// Check if PID file exists
	pidFile := "/var/ndaivimanuales/container/ndaivi.pid"
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		fmt.Println("NDAIVI Container Application is not running")
		return
	}

	// Parse PID
	pid := 0
	_, err = fmt.Sscanf(string(pidBytes), "%d", &pid)
	if err != nil || pid == 0 {
		fmt.Println("Invalid PID file, NDAIVI Container Application may not be running")
		return
	}

	// Check if process is running
	process, err := os.FindProcess(pid)
	if err != nil {
		fmt.Println("NDAIVI Container Application is not running")
		return
	}

	// Send signal 0 to check if process exists
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		fmt.Println("NDAIVI Container Application is not running")
		return
	}

	// Check log file for recent activity
	logFile := "/var/ndaivimanuales/container/logs/ndaivi.log"
	info, err := os.Stat(logFile)
	if err == nil {
		lastModified := info.ModTime()
		elapsed := time.Since(lastModified)

		fmt.Printf("NDAIVI Container Application is running with PID %d\n", pid)
		fmt.Printf("Last log activity: %s ago\n", elapsed.Round(time.Second))

		// Read the last few lines of the log file
		tail, err := exec.Command("tail", "-n", "5", logFile).Output()
		if err == nil {
			fmt.Println("\nRecent log entries:")
			fmt.Println(string(tail))
		}
	} else {
		fmt.Printf("NDAIVI Container Application is running with PID %d (no log file found)\n", pid)
	}
}

// setupLogging configures logging to file
func setupLogging() *os.File {
	// Create logs directory if it doesn't exist
	logsDir := "/var/ndaivimanuales/container/logs"
	err := os.MkdirAll(logsDir, 0755)
	if err != nil {
		fmt.Printf("Error creating logs directory: %v\n", err)
		return nil
	}

	// Open log file
	logFile := filepath.Join(logsDir, "ndaivi.log")
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error opening log file: %v\n", err)
		return nil
	}

	// Set log output to file
	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	return file
}
