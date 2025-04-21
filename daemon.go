package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// DaemonCommand represents the command to execute for the daemon
type DaemonCommand string

const (
	Start  DaemonCommand = "start"
	Stop   DaemonCommand = "stop"
	Status DaemonCommand = "status"
)

// RunAsDaemon handles daemon operations (start, stop, status)
func RunAsDaemon(cmd DaemonCommand, configPath string) error {
	// Load configuration
	config, err := loadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Ensure PID file directory exists
	pidDir := filepath.Dir(config.PidFile)
	if err := os.MkdirAll(pidDir, 0755); err != nil {
		return fmt.Errorf("failed to create PID directory: %w", err)
	}

	switch cmd {
	case Start:
		return startDaemon(config)
	case Stop:
		return stopDaemon(config)
	case Status:
		return checkDaemonStatus(config)
	default:
		return fmt.Errorf("unknown daemon command: %s", cmd)
	}
}

// startDaemon starts the NDAIVI daemon process
func startDaemon(config Config) error {
	// Check if daemon is already running
	if isRunning, pid := isDaemonRunning(config.PidFile); isRunning {
		return fmt.Errorf("daemon is already running with PID %d", pid)
	}

	// Prepare the command to run the daemon
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	// Create the daemon process
	cmd := exec.Command(execPath, "run")
	
	// Detach process from terminal
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	// Redirect standard file descriptors to /dev/null
	nullFile, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("failed to open /dev/null: %w", err)
	}
	defer nullFile.Close()

	cmd.Stdin = nullFile
	cmd.Stdout = nullFile
	cmd.Stderr = nullFile

	// Start the daemon process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start daemon: %w", err)
	}

	// Write PID to file
	pid := cmd.Process.Pid
	if err := os.WriteFile(config.PidFile, []byte(strconv.Itoa(pid)), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	fmt.Printf("NDAIVI daemon started with PID %d\n", pid)
	return nil
}

// stopDaemon stops the NDAIVI daemon process
func stopDaemon(config Config) error {
	// Check if daemon is running
	isRunning, pid := isDaemonRunning(config.PidFile)
	if !isRunning {
		return fmt.Errorf("daemon is not running")
	}

	// Send SIGTERM to the daemon process
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process with PID %d: %w", pid, err)
	}

	// Send termination signal
	fmt.Printf("Stopping NDAIVI daemon (PID %d)...\n", pid)
	if err := process.Signal(syscall.SIGTERM); err != nil {
		// If we can't send SIGTERM, try to force kill
		fmt.Printf("Failed to send SIGTERM, attempting to force kill...\n")
		if err := process.Kill(); err != nil {
			return fmt.Errorf("failed to kill process: %w", err)
		}
	}

	// Wait for process to terminate (with timeout)
	for i := 0; i < 10; i++ {
		// Check if process still exists
		if !processExists(pid) {
			// Process terminated, remove PID file
			os.Remove(config.PidFile)
			fmt.Printf("NDAIVI daemon stopped\n")
			return nil
		}

		// Wait a bit before checking again
		fmt.Printf("Waiting for daemon to terminate...\n")
		time.Sleep(1 * time.Second)
	}

	// If we get here, the process didn't terminate in time
	fmt.Printf("Daemon didn't terminate in time, attempting to force kill...\n")
	if err := process.Kill(); err != nil {
		return fmt.Errorf("failed to force kill process: %w", err)
	}

	// Remove PID file
	os.Remove(config.PidFile)
	fmt.Printf("NDAIVI daemon forcefully stopped\n")
	return nil
}

// checkDaemonStatus checks if the daemon is running
func checkDaemonStatus(config Config) error {
	isRunning, pid := isDaemonRunning(config.PidFile)
	if isRunning {
		fmt.Printf("NDAIVI daemon is running with PID %d\n", pid)
	} else {
		fmt.Printf("NDAIVI daemon is not running\n")
	}
	return nil
}

// isDaemonRunning checks if the daemon is running by checking the PID file
func isDaemonRunning(pidFile string) (bool, int) {
	// Check if PID file exists
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return false, 0
	}

	// Parse PID from file
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return false, 0
	}

	// Check if process with this PID exists
	return processExists(pid), pid
}

// processExists checks if a process with the given PID exists
func processExists(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// On Unix systems, FindProcess always succeeds, so we need to send
	// a signal 0 to check if the process exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// findAndKillChildProcesses finds and kills all child processes of the given PID
func findAndKillChildProcesses(parentPid int) {
	// Get all processes
	files, err := os.ReadDir("/proc")
	if err != nil {
		fmt.Printf("Error reading /proc: %v\n", err)
		return
	}

	// Look for child processes
	for _, file := range files {
		// Skip non-numeric entries (not processes)
		pid, err := strconv.Atoi(file.Name())
		if err != nil {
			continue
		}

		// Read process status to get parent PID
		statusFile := fmt.Sprintf("/proc/%d/status", pid)
		statusData, err := os.ReadFile(statusFile)
		if err != nil {
			continue // Process might have terminated
		}

		// Parse status file to find parent PID
		statusLines := strings.Split(string(statusData), "\n")
		for _, line := range statusLines {
			if strings.HasPrefix(line, "PPid:") {
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					ppid, err := strconv.Atoi(parts[1])
					if err != nil {
						continue
					}

					// If this is a child of our parent process, kill it
					if ppid == parentPid {
						fmt.Printf("Killing child process %d\n", pid)
						process, err := os.FindProcess(pid)
						if err != nil {
							continue
						}

						// First try SIGTERM
						process.Signal(syscall.SIGTERM)

						// Wait a bit and then force kill if still running
						time.Sleep(2 * time.Second)
						if processExists(pid) {
							process.Kill()
						}
					}
				}
				break
			}
		}
	}
}
