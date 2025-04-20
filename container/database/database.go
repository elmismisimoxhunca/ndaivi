package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/ndaivi/container/config"
)

// PostgresDB represents a PostgreSQL database connection
type PostgresDB struct {
	db     *sql.DB
	config config.DatabaseConfig
}

// Manufacturer represents a manufacturer in the database
type Manufacturer struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Category represents a product category in the database
type Category struct {
	ID            int    `json:"id"`
	ManufacturerID int    `json:"manufacturer_id"`
	Name          string `json:"name"`
	Language      string `json:"language"`
}

// AnalyzerResult represents an analyzer result in the database
type AnalyzerResult struct {
	ID            int       `json:"id"`
	URL           string    `json:"url"`
	ManufacturerID int       `json:"manufacturer_id"`
	ResultType    string    `json:"result_type"`
	Result        string    `json:"result"`
	Confidence    float64   `json:"confidence"`
	CreatedAt     time.Time `json:"created_at"`
}

// NewPostgresDB creates a new PostgreSQL database connection
func NewPostgresDB(config config.DatabaseConfig) (*PostgresDB, error) {
	// Create connection string
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.DBName, config.SSLMode,
	)

	// Connect to database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test connection
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create PostgresDB instance
	pgdb := &PostgresDB{
		db:     db,
		config: config,
	}

	// Initialize database schema
	err = pgdb.InitSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database schema: %w", err)
	}

	return pgdb, nil
}

// Close closes the database connection
func (p *PostgresDB) Close() error {
	return p.db.Close()
}

// InitSchema initializes the database schema
func (p *PostgresDB) InitSchema() error {
	// Create manufacturers table
	_, err := p.db.Exec(`
		CREATE TABLE IF NOT EXISTS manufacturers (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL UNIQUE,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create manufacturers table: %w", err)
	}

	// Create categories table
	_, err = p.db.Exec(`
		CREATE TABLE IF NOT EXISTS categories (
			id SERIAL PRIMARY KEY,
			manufacturer_id INTEGER NOT NULL REFERENCES manufacturers(id),
			name VARCHAR(255) NOT NULL,
			language VARCHAR(10) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			UNIQUE(manufacturer_id, name, language)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create categories table: %w", err)
	}

	// Create analyzer_results table
	_, err = p.db.Exec(`
		CREATE TABLE IF NOT EXISTS analyzer_results (
			id SERIAL PRIMARY KEY,
			url TEXT NOT NULL,
			manufacturer_id INTEGER REFERENCES manufacturers(id),
			result_type VARCHAR(50) NOT NULL,
			result TEXT NOT NULL,
			confidence FLOAT,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create analyzer_results table: %w", err)
	}

	// Create stats table
	_, err = p.db.Exec(`
		CREATE TABLE IF NOT EXISTS stats (
			id SERIAL PRIMARY KEY,
			stat_type VARCHAR(50) NOT NULL,
			stat_value JSONB NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create stats table: %w", err)
	}

	return nil
}

// GetOrCreateManufacturer gets or creates a manufacturer
func (p *PostgresDB) GetOrCreateManufacturer(name string) (int, error) {
	// Check if manufacturer exists
	var id int
	err := p.db.QueryRow("SELECT id FROM manufacturers WHERE name = $1", name).Scan(&id)
	if err == nil {
		// Manufacturer exists
		return id, nil
	} else if err != sql.ErrNoRows {
		// Error occurred
		return 0, fmt.Errorf("failed to query manufacturer: %w", err)
	}

	// Manufacturer doesn't exist, create it
	err = p.db.QueryRow("INSERT INTO manufacturers (name) VALUES ($1) RETURNING id", name).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to create manufacturer: %w", err)
	}

	return id, nil
}

// AddCategory adds a category to the database
func (p *PostgresDB) AddCategory(manufacturerID int, name string, language string) (int, error) {
	// Check if category exists
	var id int
	err := p.db.QueryRow("SELECT id FROM categories WHERE manufacturer_id = $1 AND name = $2 AND language = $3",
		manufacturerID, name, language).Scan(&id)
	if err == nil {
		// Category exists
		return id, nil
	} else if err != sql.ErrNoRows {
		// Error occurred
		return 0, fmt.Errorf("failed to query category: %w", err)
	}

	// Category doesn't exist, create it
	err = p.db.QueryRow("INSERT INTO categories (manufacturer_id, name, language) VALUES ($1, $2, $3) RETURNING id",
		manufacturerID, name, language).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to create category: %w", err)
	}

	return id, nil
}

// AddAnalyzerResult adds an analyzer result to the database
func (p *PostgresDB) AddAnalyzerResult(url string, manufacturerID int, resultType string, result string, confidence float64) (int, error) {
	// Insert analyzer result
	var id int
	err := p.db.QueryRow(
		"INSERT INTO analyzer_results (url, manufacturer_id, result_type, result, confidence) VALUES ($1, $2, $3, $4, $5) RETURNING id",
		url, manufacturerID, resultType, result, confidence).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to create analyzer result: %w", err)
	}

	return id, nil
}

// GetManufacturers gets all manufacturers from the database
func (p *PostgresDB) GetManufacturers() ([]Manufacturer, error) {
	// Query manufacturers
	rows, err := p.db.Query("SELECT id, name FROM manufacturers ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("failed to query manufacturers: %w", err)
	}
	defer rows.Close()

	// Parse results
	var manufacturers []Manufacturer
	for rows.Next() {
		var m Manufacturer
		err := rows.Scan(&m.ID, &m.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to scan manufacturer: %w", err)
		}
		manufacturers = append(manufacturers, m)
	}

	return manufacturers, nil
}

// GetCategories gets all categories for a manufacturer from the database
func (p *PostgresDB) GetCategories(manufacturerID int, language string) ([]Category, error) {
	// Query categories
	rows, err := p.db.Query("SELECT id, manufacturer_id, name, language FROM categories WHERE manufacturer_id = $1 AND language = $2 ORDER BY name",
		manufacturerID, language)
	if err != nil {
		return nil, fmt.Errorf("failed to query categories: %w", err)
	}
	defer rows.Close()

	// Parse results
	var categories []Category
	for rows.Next() {
		var c Category
		err := rows.Scan(&c.ID, &c.ManufacturerID, &c.Name, &c.Language)
		if err != nil {
			return nil, fmt.Errorf("failed to scan category: %w", err)
		}
		categories = append(categories, c)
	}

	return categories, nil
}

// SaveStats saves statistics to the database
func (p *PostgresDB) SaveStats(statType string, statValue string) error {
	// Insert stats
	_, err := p.db.Exec("INSERT INTO stats (stat_type, stat_value) VALUES ($1, $2)", statType, statValue)
	if err != nil {
		return fmt.Errorf("failed to save stats: %w", err)
	}

	return nil
}

// GetStats gets the latest statistics from the database
func (p *PostgresDB) GetStats(statType string) (string, error) {
	// Query stats
	var statValue string
	err := p.db.QueryRow("SELECT stat_value FROM stats WHERE stat_type = $1 ORDER BY created_at DESC LIMIT 1", statType).Scan(&statValue)
	if err != nil {
		if err == sql.ErrNoRows {
			return "{}", nil
		}
		return "", fmt.Errorf("failed to query stats: %w", err)
	}

	return statValue, nil
}
