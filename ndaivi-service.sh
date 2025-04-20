#!/bin/bash

# NDAIVI Service Manager
# Controls the NDAIVI crawler and analyzer system

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/config.yaml"
PID_FILE="$SCRIPT_DIR/logs/ndaivi.pid"
LOG_FILE="$SCRIPT_DIR/logs/ndaivi.log"
STATUS_FILE="$SCRIPT_DIR/logs/ndaivi_status.json"

# Ensure logs directory exists
mkdir -p "$SCRIPT_DIR/logs"
mkdir -p "$SCRIPT_DIR/scraper/data"

function start() {
    echo "Starting NDAIVI daemon..."
    
    # Check if already running
    if [ -f "$PID_FILE" ] && ps -p $(cat "$PID_FILE") > /dev/null; then
        echo "NDAIVI is already running with PID $(cat "$PID_FILE")"
        return 1
    fi
    
    # Ensure directories exist
    mkdir -p "$(dirname "$LOG_FILE")"
    mkdir -p "$SCRIPT_DIR/scraper/data"
    
    # Export environment variables for crawler
    export NDAIVI_TARGET_WEBSITE="manualslib.com"
    export NDAIVI_MAX_URLS="1000"
    export NDAIVI_LOG_FILE="$LOG_FILE"
    export NDAIVI_DB_PATH="$SCRIPT_DIR/scraper/data/crawler.db"
    
    # Start daemon with nohup to keep running after terminal closes
    cd "$SCRIPT_DIR"
    nohup ./ndaivi --start-daemon --config "$CONFIG_FILE" > /dev/null 2>&1 &
    
    # Wait for PID file to be created
    echo "Waiting for daemon to initialize..."
    for i in {1..10}; do
        if [ -f "$PID_FILE" ]; then
            echo "NDAIVI daemon started with PID $(cat "$PID_FILE")"
            sleep 2  # Give it time to create the status file
            status   # Show initial status
            return 0
        fi
        echo -n "."
        sleep 1
    done
    
    echo "
Failed to start NDAIVI daemon or confirm PID"
    return 1
}

function stop() {
    echo "Stopping NDAIVI daemon..."
    
    # Check if running
    if [ ! -f "$PID_FILE" ]; then
        echo "NDAIVI is not running (no PID file)"
        return 0
    fi
    
    PID=$(cat "$PID_FILE")
    if ! ps -p "$PID" > /dev/null; then
        echo "NDAIVI is not running (stale PID file)"
        rm -f "$PID_FILE"
        return 0
    fi
    
    # Send SIGTERM to the process
    kill -15 "$PID"
    
    # Wait for process to terminate
    for i in {1..10}; do
        if ! ps -p "$PID" > /dev/null; then
            echo "NDAIVI daemon stopped"
            rm -f "$PID_FILE"
            return 0
        fi
        sleep 1
    done
    
    # Force kill if still running
    echo "NDAIVI daemon did not stop gracefully, forcing termination"
    kill -9 "$PID"
    rm -f "$PID_FILE"
    return 0
}

function status() {
    # Use the status tool to display current status
    if [ -f "$SCRIPT_DIR/ndaivi-status" ]; then
        "$SCRIPT_DIR/ndaivi-status" --verbose --log 20
    else
        echo "NDAIVI Status Tool"
        echo "==============="
        
        # Check if PID file exists and process is running
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if ps -p "$PID" > /dev/null; then
                echo "Status: Running (PID: $PID)"
            else
                echo "Status: Not running (stale PID file)"
            fi
        else
            echo "Status: Not running"
        fi
        
        # Show stats file if it exists
        if [ -f "$STATUS_FILE" ]; then
            echo "
Status File Content:"
            cat "$STATUS_FILE" | python3 -m json.tool
        fi
        
        # Show recent logs
        if [ -f "$LOG_FILE" ]; then
            echo "
Recent Logs:"
            tail -n 20 "$LOG_FILE"
        fi
    fi
}

function logs() {
    # Display log file with optional lines count
    LINES=${1:-20}
    if [ -f "$LOG_FILE" ]; then
        echo "=== Last $LINES lines of $LOG_FILE ==="
        tail -n "$LINES" "$LOG_FILE"
    else
        echo "Log file not found: $LOG_FILE"
    fi
}

function follow_logs() {
    # Follow log file updates
    tail -f "$LOG_FILE"
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 2
        start
        ;;
    status)
        status
        ;;
    logs)
        logs "$2"
        ;;
    follow)
        follow_logs
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs [lines]|follow}"
        exit 1
        ;;
esac

exit 0
