package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ndaivi/container/config"
)

// Client represents a Redis client
type Client struct {
	client  *redis.Client
	ctx     context.Context
	config  config.RedisConfig
	channels config.ChannelsConfig
}

// PubSub represents a Redis subscription
type PubSub struct {
	pubsub *redis.PubSub
	handler func([]byte)
}

// Message represents a Redis message
type Message struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// NewClient creates a new Redis client
func NewClient(redisConfig config.RedisConfig) (*Client, error) {
	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", redisConfig.Host, redisConfig.Port),
		Password: redisConfig.Password,
		DB:       redisConfig.DB,
	})

	// Create context
	ctx := context.Background()

	// Test connection
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Client{
		client:  client,
		ctx:     ctx,
		config:  redisConfig,
	}, nil
}

// SetChannels sets the Redis channels configuration
func (c *Client) SetChannels(channels config.ChannelsConfig) {
	c.channels = channels
}

// Close closes the Redis client
func (c *Client) Close() error {
	return c.client.Close()
}

// Publish publishes a message to a Redis channel
func (c *Client) Publish(channel string, message interface{}) error {
	// Convert message to JSON
	msgBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Publish message
	err = c.client.Publish(c.ctx, channel, msgBytes).Err()
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Subscribe subscribes to a Redis channel
func (c *Client) Subscribe(channel string, handler func([]byte)) (*PubSub, error) {
	// Create PubSub
	pubsub := c.client.Subscribe(c.ctx, channel)

	// Start goroutine to handle messages
	go func() {
		for {
			msg, err := pubsub.ReceiveMessage(c.ctx)
			if err != nil {
				log.Printf("Error receiving message: %v", err)
				time.Sleep(time.Second)
				continue
			}

			// Handle message
			handler([]byte(msg.Payload))
		}
	}()

	return &PubSub{pubsub: pubsub, handler: handler}, nil
}

// Close closes a Redis subscription
func (p *PubSub) Close() error {
	return p.pubsub.Close()
}

// PublishCommand publishes a command to a Redis channel
func (c *Client) PublishCommand(channel string, commandType string, payload interface{}) error {
	message := Message{
		Type:    commandType,
		Payload: payload,
	}

	return c.Publish(channel, message)
}

// GetBacklogStatus gets the current backlog status
func (c *Client) GetBacklogStatus() (map[string]interface{}, error) {
	// Create a channel to receive the response
	respCh := make(chan map[string]interface{}, 1)

	// Subscribe to the backlog status channel
	pubsub, err := c.Subscribe(c.channels.BacklogStatus, func(data []byte) {
		// Parse the response
		var response map[string]interface{}
		err := json.Unmarshal(data, &response)
		if err != nil {
			log.Printf("Error parsing backlog status: %v", err)
			return
		}

		// Send the response to the channel
		respCh <- response
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to backlog status channel: %w", err)
	}
	defer pubsub.Close()

	// Request the backlog status
	err = c.PublishCommand(c.channels.BacklogRequest, "get_status", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to request backlog status: %w", err)
	}

	// Wait for the response with timeout
	select {
	case response := <-respCh:
		return response, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timed out waiting for backlog status")
	}
}

// RequestBacklogBatch requests a batch of URLs from the backlog
func (c *Client) RequestBacklogBatch(batchSize int) error {
	// Request the backlog batch
	return c.PublishCommand(c.channels.BacklogRequest, "get_batch", map[string]interface{}{
		"batch_size": batchSize,
	})
}

// MarkURLProcessed marks a URL as processed
func (c *Client) MarkURLProcessed(url string) error {
	// Mark the URL as processed
	return c.PublishCommand(c.channels.CrawlerCommands, "mark_processed", map[string]interface{}{
		"url": url,
	})
}

// StartCrawler starts the crawler
func (c *Client) StartCrawler(startURL string, maxURLs int) error {
	// Start the crawler
	return c.PublishCommand(c.channels.CrawlerCommands, "start", map[string]interface{}{
		"start_url": startURL,
		"max_urls":  maxURLs,
	})
}

// StopCrawler stops the crawler
func (c *Client) StopCrawler() error {
	// Stop the crawler
	return c.PublishCommand(c.channels.CrawlerCommands, "stop", nil)
}

// StartAnalyzer starts the analyzer
func (c *Client) StartAnalyzer() error {
	// Start the analyzer
	return c.PublishCommand(c.channels.AnalyzerCommands, "start", nil)
}

// StopAnalyzer stops the analyzer
func (c *Client) StopAnalyzer() error {
	// Stop the analyzer
	return c.PublishCommand(c.channels.AnalyzerCommands, "stop", nil)
}

// PublishContainerStatus publishes the container status
func (c *Client) PublishContainerStatus(status map[string]interface{}) error {
	// Publish the container status
	return c.Publish(c.channels.ContainerStatus, status)
}
