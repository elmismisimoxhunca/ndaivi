#!/bin/bash

# Kill any existing container process
pkill -f ndaivi-container

# Remove the database to start fresh
rm -rf /var/ndaivimanuales/scraper/data/crawler.db*

# Start the container application
cd /var/ndaivimanuales/container && ./ndaivi-container -daemon

# Wait a moment for it to initialize
sleep 5

# Check the status
/var/ndaivimanuales/container/check_status.sh

# Wait a bit longer to see if the crawler is working
sleep 10

# Check the status again
/var/ndaivimanuales/container/check_status.sh

echo "\nTest complete. Check the logs above to see if the crawler is working properly."
