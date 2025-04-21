package main

import (
	"fmt"
)

// checkForCrawledData verifies if there is data in the SQLite database to analyze
func (n *NDAIVI) checkForCrawledData() (bool, error) {
	// Check if SQLite database exists and has data
	if n.sqliteDB == nil {
		return false, fmt.Errorf("SQLite database connection not initialized")
	}

	// Check if pages table exists and has data
	var count int
	query := "SELECT COUNT(*) FROM pages"
	err := n.sqliteDB.QueryRow(query).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("error checking for data in SQLite database: %w", err)
	}

	n.mainLogger.Printf("Found %d pages in the SQLite database", count)
	return count > 0, nil
}
