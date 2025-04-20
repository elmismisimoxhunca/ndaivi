#!/bin/bash

# Kill any existing container process
pkill -f ndaivi-container

# Remove the database to start fresh
rm -rf /var/ndaivimanuales/scraper/data/crawler.db*

# Update the config to use lower thresholds for testing
sed -i 's/backlog_min_threshold: 10/backlog_min_threshold: 2/g' /var/ndaivimanuales/config.yaml
sed -i 's/backlog_target_size: 50/backlog_target_size: 5/g' /var/ndaivimanuales/config.yaml

# Start the container application
cd /var/ndaivimanuales/container && ./ndaivi-container -daemon

# Wait a moment for it to initialize
sleep 5

# Check the status
/var/ndaivimanuales/container/check_status.sh

# Wait a bit longer to see if the crawler is working
sleep 15

# Check the status again
/var/ndaivimanuales/container/check_status.sh

echo "\nTest complete. Check the logs above to see if the crawler is working properly."
