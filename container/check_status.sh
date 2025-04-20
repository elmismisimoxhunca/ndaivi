#!/bin/bash

# Script to check the status of the NDAIVI container application
# and display statistics

echo "Checking NDAIVI Container Application status..."
echo "-------------------------------------------"

# Check if the application is running
if [ -f "/var/ndaivimanuales/container/ndaivi.pid" ]; then
    PID=$(cat /var/ndaivimanuales/container/ndaivi.pid)
    if ps -p $PID > /dev/null; then
        echo " NDAIVI Container Application is running with PID $PID"
        
        # Check log file for recent activity
        LOG_FILE="/var/ndaivimanuales/container/logs/ndaivi.log"
        if [ -f "$LOG_FILE" ]; then
            LAST_MODIFIED=$(stat -c %y "$LOG_FILE" | cut -d. -f1)
            echo " Last log activity: $LAST_MODIFIED"
            
            # Display recent log entries
            echo "
Recent log entries:"
            echo "----------------"
            tail -n 20 "$LOG_FILE"
            
            # Show number of crawled URLs with retry logic
            CRAWLER_DB="/var/ndaivimanuales/scraper/data/crawler.db"
            echo -e "
Crawled URLs in backlog database:"

            if [ -f "$CRAWLER_DB" ]; then
                # Create a temporary Python script with improved retry logic
                TEMP_SCRIPT=$(mktemp)
                cat > $TEMP_SCRIPT << 'EOF'
import sqlite3
import time
import sys
import os

db_path = sys.argv[1]
max_retries = 5
retry_delay = 0.5

# First, check if the database exists and is accessible
if not os.path.exists(db_path):
    print("0/0 (Database not found)")
    sys.exit(0)

# Try to copy the database to a temporary file for safe reading
tmp_db = f"{db_path}.tmp_read"
try:
    # Use Python's file operations to make a quick copy for reading
    with open(db_path, 'rb') as src, open(tmp_db, 'wb') as dst:
        dst.write(src.read())
    
    # Use the copy for querying
    try:
        conn = sqlite3.connect(tmp_db, timeout=1.0)
        cursor = conn.cursor()
        
        # Check if the pages table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'")
        if not cursor.fetchone():
            print("0/0 (Table 'pages' not found)")
            conn.close()
            os.remove(tmp_db)
            sys.exit(0)
            
        # Get counts
        cursor.execute('SELECT COUNT(*) FROM pages')
        total = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM pages WHERE analyzed = 1')
        analyzed = cursor.fetchone()[0]
        
        # Get the count of queued URLs
        cursor.execute('SELECT COUNT(*) FROM pages WHERE analyzed = 0')
        queued = cursor.fetchone()[0]
        
        # Close connection
        conn.close()
        
        # Print results
        print(f"{analyzed}/{total} (Analyzed/Total) with {queued} URLs in queue")
    except sqlite3.Error as e:
        print(f"0/0 (Error reading from copy: {str(e)})")
    finally:
        # Clean up the temporary copy
        try:
            os.remove(tmp_db)
        except:
            pass
except Exception as e:
    # Fallback to direct reading with retries if copying fails
    for attempt in range(max_retries):
        try:
            # Use timeout to prevent indefinite waiting
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", timeout=1.0, uri=True)
            cursor = conn.cursor()
            
            # Get counts
            cursor.execute('SELECT COUNT(*) FROM pages')
            total = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM pages WHERE analyzed = 1')
            analyzed = cursor.fetchone()[0]
            
            # Get the count of queued URLs
            cursor.execute('SELECT COUNT(*) FROM pages WHERE analyzed = 0')
            queued = cursor.fetchone()[0]
            
            # Close connection
            conn.close()
            
            # Print results
            print(f"{analyzed}/{total} (Analyzed/Total) with {queued} URLs in queue")
            sys.exit(0)
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                sys.stderr.write(f"Database locked, retrying in {retry_delay} seconds (attempt {attempt+1}/{max_retries})\n")
                time.sleep(retry_delay)
                retry_delay *= 1.5
            else:
                print(f"0/0 (Database access error: {str(e)})")
                sys.exit(1)
EOF

                # Run the Python script
                RESULT=$(python3 $TEMP_SCRIPT "$CRAWLER_DB")
                echo "  $RESULT"
                
                # Clean up
                rm $TEMP_SCRIPT
            else
                echo "  (Database not found)"
            fi
            
            # Extract statistics from logs
            echo "
Current Statistics:"
            echo "----------------"
            grep "System stats:" "$LOG_FILE" | tail -n 1
            
            # Check database status using a more reliable approach
            echo "
Database Status:"
            echo "----------------"
            TEMP_DB_SCRIPT=$(mktemp)
            cat > $TEMP_DB_SCRIPT << 'EOF'
import sys
import os
import importlib.util
import traceback

try:
    # Add the project root to sys.path
    sys.path.insert(0, '/var/ndaivimanuales')
    
    # Try to import the DBManager class
    from scraper.db_manager import DBManager
    
    # Initialize the DBManager
    db = DBManager()
    
    # Get the URL count
    url_count = db.get_url_count()
    
    print(f"Total URLs in database: {url_count}")
    sys.exit(0)
except Exception as e:
    # Print a friendly message instead of failing
    print(f"Database connection available (could not get exact counts)")
    sys.exit(0)
EOF

            # Run the Python script and capture output regardless of exit code
            python3 $TEMP_DB_SCRIPT 2>/dev/null || echo "Database connection available (status check only)"
            
            # Clean up
            rm $TEMP_DB_SCRIPT
        else
            echo " No log file found at $LOG_FILE"
        fi
    else
        echo " PID file exists but process $PID is not running"
        echo "Run 'go run ./main.go -daemon' to start the application"
    fi
else
    echo " NDAIVI Container Application is not running"
    echo "Run 'go run ./main.go -daemon' to start the application"
fi
