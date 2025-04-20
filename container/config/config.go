package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	Languages   LanguageConfig   `yaml:"languages"`
	Translation TranslationConfig `yaml:"translation"`
	Claude      ClaudeConfig     `yaml:"claude_analyzer"`
	Keywords    KeywordConfig    `yaml:"keywords"`
	Prompts     PromptConfig     `yaml:"prompt_templates"`
	WebCrawler  WebCrawlerConfig `yaml:"web_crawler"`
	Redis       RedisConfig      `yaml:"redis"`
	Application AppConfig        `yaml:"application"`
	Database    DatabaseConfig   `yaml:"database,omitempty"`
}

// LanguageConfig represents language configuration
type LanguageConfig struct {
	Source               string            `yaml:"source"`
	Targets              map[string]string `yaml:"targets"`
	TranslationBatchSize int               `yaml:"translation_batch_size"`
}

// TranslationConfig represents translation configuration
type TranslationConfig struct {
	Enabled          bool   `yaml:"enabled"`
	DefaultSourceLang string `yaml:"default_source_lang"`
	DefaultTargetLang string `yaml:"default_target_lang"`
	BatchSize         int    `yaml:"batch_size"`
	CacheEnabled      bool   `yaml:"cache_enabled"`
	CacheTTL          int    `yaml:"cache_ttl"`
}

// ClaudeConfig represents Claude API configuration
type ClaudeConfig struct {
	APIKey               string `yaml:"api_key"`
	APIBaseURL           string `yaml:"api_base_url"`
	ClaudeModel          string `yaml:"claude_model"`
	EnableCache          bool   `yaml:"enable_cache"`
	CacheTTL             int    `yaml:"cache_ttl"`
	CacheDir             string `yaml:"cache_dir"`
	MaxTokens            int    `yaml:"max_tokens"`
	Temperature          float64 `yaml:"temperature"`
	MinConfidenceThreshold float64 `yaml:"min_confidence_threshold"`
}

// KeywordConfig represents keyword configuration
type KeywordConfig struct {
	Positive []string `yaml:"positive"`
	Negative []string `yaml:"negative"`
}

// PromptConfig represents prompt templates configuration
type PromptConfig struct {
	System           map[string]string `yaml:"system"`
	PageTypeAnalysis string            `yaml:"page_type_analysis"`
	ExtractManufacturer string         `yaml:"extract_manufacturer"`
	ExtractCategories string           `yaml:"extract_categories"`
	TranslateCategories string         `yaml:"translate_categories"`
}

// WebCrawlerConfig represents web crawler configuration
type WebCrawlerConfig struct {
	TargetWebsite         string   `yaml:"target_website"`
	MaxDepth              int      `yaml:"max_depth"`
	MaxURLs               *int     `yaml:"max_urls"`
	MaxURLsPerDomain      int      `yaml:"max_urls_per_domain"`
	MaxConcurrentRequests int      `yaml:"max_concurrent_requests"`
	RequestDelay          float64  `yaml:"request_delay"`
	Timeout               int      `yaml:"timeout"`
	UserAgent             string   `yaml:"user_agent"`
	RespectRobotsTxt      bool     `yaml:"respect_robots_txt"`
	FollowRedirects       bool     `yaml:"follow_redirects"`
	AllowedDomains        []string `yaml:"allowed_domains"`
	DisallowedDomains     []string `yaml:"disallowed_domains"`
	AllowedURLPatterns    []string `yaml:"allowed_url_patterns"`
	DisallowedURLPatterns []string `yaml:"disallowed_url_patterns"`
	MaxContentSize        int      `yaml:"max_content_size"`
	ExtractMetadata       bool     `yaml:"extract_metadata"`
	ExtractLinks          bool     `yaml:"extract_links"`
	ExtractText           bool     `yaml:"extract_text"`
	DBPath                string   `yaml:"db_path"`
	RestrictToStartDomain bool     `yaml:"restrict_to_start_domain"`
	SingleDomainMode      bool     `yaml:"single_domain_mode"`
	StatsUpdateInterval     int      `yaml:"stats_update_interval"`
	VerboseLogging         bool     `yaml:"verbose_logging"`
	MaxUnanalyzedUrls      int      `yaml:"max_unanalyzed_urls"`
	CrawlSpeedCheckInterval int      `yaml:"crawl_speed_check_interval"`
}

// RedisConfig represents Redis configuration
type RedisConfig struct {
	Host                 string `yaml:"host"`
	Port                 int    `yaml:"port"`
	DB                   int    `yaml:"db"`
	Password             string `yaml:"password"`
	SocketTimeout        int    `yaml:"socket_timeout"`
	SocketConnectTimeout int    `yaml:"socket_connect_timeout"`
	RetryOnTimeout       bool   `yaml:"retry_on_timeout"`
	HealthCheckInterval  int    `yaml:"health_check_interval"`
}

// AppConfig represents application configuration
type AppConfig struct {
	Components ComponentsConfig `yaml:"components"`
	Channels   ChannelsConfig  `yaml:"channels"`
}

// ComponentsConfig represents components configuration
type ComponentsConfig struct {
	Crawler  CrawlerConfig  `yaml:"crawler"`
	Analyzer AnalyzerConfig `yaml:"analyzer"`
	Stats    StatsConfig    `yaml:"stats"`
}

// CrawlerConfig represents crawler component configuration
type CrawlerConfig struct {
	Enabled       bool `yaml:"enabled"`
	WorkerThreads int  `yaml:"worker_threads"`
	AutoStart     bool `yaml:"auto_start"`
	ResumeFromDB  bool `yaml:"resume_from_db"`
}

// AnalyzerConfig represents analyzer component configuration
type AnalyzerConfig struct {
	Enabled       bool `yaml:"enabled"`
	WorkerThreads int  `yaml:"worker_threads"`
	BatchSize     int  `yaml:"batch_size"`
}

// StatsConfig represents stats component configuration
type StatsConfig struct {
	Enabled        bool `yaml:"enabled"`
	UpdateInterval int  `yaml:"update_interval"`
}

// ChannelsConfig represents Redis channels configuration
type ChannelsConfig struct {
	CrawlerCommands  string `yaml:"crawler_commands"`
	CrawlerStatus    string `yaml:"crawler_status"`
	AnalyzerCommands string `yaml:"analyzer_commands"`
	AnalyzerStatus   string `yaml:"analyzer_status"`
	BacklogStatus    string `yaml:"backlog_status,omitempty"`
	BacklogRequest   string `yaml:"backlog_request,omitempty"`
	ContainerStatus  string `yaml:"container_status,omitempty"`
	AnalyzerResults  string `yaml:"analyzer_results,omitempty"`
	Stats            string `yaml:"stats,omitempty"`
	System           string `yaml:"system,omitempty"`
}

// DatabaseConfig represents PostgreSQL database configuration
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(configPath string) (*Config, error) {
	// Read the YAML file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse the YAML data
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set default values for database if not provided
	if config.Database.Host == "" {
		config.Database.Host = "localhost"
	}
	if config.Database.Port == 0 {
		config.Database.Port = 5432
	}
	if config.Database.User == "" {
		config.Database.User = "postgres"
	}
	if config.Database.DBName == "" {
		config.Database.DBName = "ndaivi"
	}
	if config.Database.SSLMode == "" {
		config.Database.SSLMode = "disable"
	}

	// Set default values for Redis channels if not provided
	if config.Application.Channels.BacklogStatus == "" {
		config.Application.Channels.BacklogStatus = "ndaivi:backlog:status"
	}
	if config.Application.Channels.BacklogRequest == "" {
		config.Application.Channels.BacklogRequest = "ndaivi:backlog:request"
	}
	if config.Application.Channels.ContainerStatus == "" {
		config.Application.Channels.ContainerStatus = "ndaivi:container:status"
	}
	if config.Application.Channels.AnalyzerResults == "" {
		config.Application.Channels.AnalyzerResults = "ndaivi:analyzer:results"
	}
	if config.Application.Channels.Stats == "" {
		config.Application.Channels.Stats = "ndaivi:stats"
	}
	if config.Application.Channels.System == "" {
		config.Application.Channels.System = "ndaivi:system"
	}

	return &config, nil
}
