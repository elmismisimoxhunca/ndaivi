package coordinator

import (
	"testing"
	"time"
	"github.com/ndaivi/container/config"
)

func TestCoordinatorSpeedControl(t *testing.T) {
	// Create test config
	cfg := &config.Config{
		WebCrawler: config.WebCrawlerConfig{
			MaxUnanalyzedUrls:       100,  // Small number for testing
			CrawlSpeedCheckInterval: 1,    // Check every second for testing
			DBPath:                 "/tmp/test_crawler.db",
		},
	}

	// Create coordinator
	coord := NewCoordinator(cfg, nil)
	if coord == nil {
		t.Fatal("Failed to create coordinator")
	}

	// Start coordinator
	coord.Run()
	defer coord.Stop()

	// Wait for components to start
	time.Sleep(2 * time.Second)

	// Test speed control
	tests := []struct {
		name          string
		unanalyzedURLs int
		expectedSpeed string
	}{
		{
			name:          "Normal speed when below threshold",
			unanalyzedURLs: 50,
			expectedSpeed: "normal",
		},
		{
			name:          "Slow speed when above threshold",
			unanalyzedURLs: 150,
			expectedSpeed: "slow",
		},
		{
			name:          "Back to normal when drops below threshold",
			unanalyzedURLs: 70,
			expectedSpeed: "normal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate URL count by directly setting stats
			coord.stats.mutex.Lock()
			coord.stats.urlsQueued = tt.unanalyzedURLs
			coord.stats.mutex.Unlock()

			// Wait for speed control to take effect
			time.Sleep(2 * time.Second)

			// Check crawler speed
			coord.crawlerProcess.mutex.Lock()
			actualSpeed := "normal"
			if coord.crawlerProcess.slowMode {
				actualSpeed = "slow"
			}
			coord.crawlerProcess.mutex.Unlock()

			if actualSpeed != tt.expectedSpeed {
				t.Errorf("Expected speed %s, got %s", tt.expectedSpeed, actualSpeed)
			}
		})
	}
}

func TestCoordinatorCommands(t *testing.T) {
	// Create test config
	cfg := &config.Config{
		WebCrawler: config.WebCrawlerConfig{
			DBPath: "/tmp/test_crawler.db",
		},
	}

	// Create coordinator
	coord := NewCoordinator(cfg, nil)
	if coord == nil {
		t.Fatal("Failed to create coordinator")
	}

	// Start coordinator
	coord.Run()
	defer coord.Stop()

	// Wait for components to start
	time.Sleep(2 * time.Second)

	// Test setCrawlerSpeed
	tests := []struct {
		name        string
		speed       string
		expectError bool
	}{
		{
			name:        "Set normal speed",
			speed:       "normal",
			expectError: false,
		},
		{
			name:        "Set slow speed",
			speed:       "slow",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := coord.setCrawlerSpeed(tt.speed)
			if (err != nil) != tt.expectError {
				t.Errorf("setCrawlerSpeed() error = %v, expectError %v", err, tt.expectError)
			}

			// Verify speed was set correctly
			coord.crawlerProcess.mutex.Lock()
			actualSpeed := "normal"
			if coord.crawlerProcess.slowMode {
				actualSpeed = "slow"
			}
			coord.crawlerProcess.mutex.Unlock()

			if actualSpeed != tt.speed {
				t.Errorf("Expected speed %s, got %s", tt.speed, actualSpeed)
			}
		})
	}
}

func TestCoordinatorStats(t *testing.T) {
	// Create test config
	cfg := &config.Config{
		WebCrawler: config.WebCrawlerConfig{
			DBPath: "/tmp/test_crawler.db",
		},
	}

	// Create coordinator
	coord := NewCoordinator(cfg, nil)
	if coord == nil {
		t.Fatal("Failed to create coordinator")
	}

	// Start coordinator
	coord.Run()
	defer coord.Stop()

	// Wait for components to start
	time.Sleep(2 * time.Second)

	// Set some test stats
	coord.stats.mutex.Lock()
	coord.stats.urlsProcessed = 100
	coord.stats.urlsQueued = 50
	coord.stats.urlsFailed = 10
	coord.stats.mutex.Unlock()

	// Test getDatabaseStats
	_, total, err := coord.getDatabaseStats()
	if err != nil {
		t.Errorf("getDatabaseStats() error = %v", err)
	}

	// Since we're using memory stats as fallback, verify those
	if total != 150 { // processed + queued
		t.Errorf("Expected total URLs 150, got %d", total)
	}
}
