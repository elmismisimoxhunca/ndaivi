# NDAIVI Container Application Makefile

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOLINT=golint

# Binary names
BINARY_NAME=ndaivi-container
CLI_BINARY_NAME=ndaivi-cli

# Build directory
BUILD_DIR=build

# Main build target
all: clean deps build

# Build the application
build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) -v ./main.go
	$(GOBUILD) -o $(BUILD_DIR)/$(CLI_BINARY_NAME) -v ./cmd/ndaivi-container/main.go

# Test the application
test:
	$(GOTEST) -v ./...

# Clean build files
clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

# Install dependencies
deps:
	$(GOGET) -v -t ./...

# Run the application
run: build
	./$(BUILD_DIR)/$(BINARY_NAME)

# Run the CLI
cli: build
	./$(BUILD_DIR)/$(CLI_BINARY_NAME)

# Install the application
install: build
	cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/
	cp $(BUILD_DIR)/$(CLI_BINARY_NAME) /usr/local/bin/

.PHONY: all build test clean deps run cli install
