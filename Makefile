.PHONY: build clean test install deps lint fmt vet web web-dev server

# Binary name
BINARY_NAME=pgsync
BUILD_DIR=./bin

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Web parameters
WEB_DIR=./web
NPM=npm

# E2E database connections
SOURCE_DB=postgres://postgres:postgres@localhost:5433/crm_source?sslmode=disable
TARGET_DB=postgres://postgres:postgres@localhost:5434/crm_target?sslmode=disable

# Build flags
LDFLAGS=-ldflags "-s -w"
BUILD_FLAGS=-trimpath $(LDFLAGS)

all: deps fmt vet test web build

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(BUILD_FLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) .

build-linux:
	@echo "Building $(BINARY_NAME) for Linux..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(BUILD_FLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 .

build-windows:
	@echo "Building $(BINARY_NAME) for Windows..."
	@mkdir -p $(BUILD_DIR)
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(BUILD_FLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe .

build-darwin:
	@echo "Building $(BINARY_NAME) for macOS..."
	@mkdir -p $(BUILD_DIR)
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(BUILD_FLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(BUILD_FLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 .

build-all: build-linux build-windows build-darwin

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILD_DIR)
	@rm -rf $(WEB_DIR)/dist
	@rm -rf $(WEB_DIR)/node_modules
	@rm -f *.db

# Web UI targets
web: web-deps web-build

web-deps:
	@echo "Installing web dependencies..."
	@cd $(WEB_DIR) && $(NPM) install

web-build: web-deps
	@echo "Building web UI..."
	@cd $(WEB_DIR) && $(NPM) run build

web-dev:
	@echo "Starting web dev server..."
	@cd $(WEB_DIR) && $(NPM) run dev

# Server mode (requires web build)
server: web build
	@echo "Starting pgsync server..."
	$(BUILD_DIR)/$(BINARY_NAME) -server \
		-source "$(SOURCE_DB)" \
		-target "$(TARGET_DB)"

server-dev:
	@echo "Starting pgsync server (dev mode, no web build)..."
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) . && \
	$(BUILD_DIR)/$(BINARY_NAME) -server \
		-source "$(SOURCE_DB)" \
		-target "$(TARGET_DB)"

test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) verify

deps-update:
	@echo "Updating dependencies..."
	$(GOMOD) tidy
	$(GOGET) -u ./...

fmt:
	@echo "Formatting code..."
	$(GOFMT) -s -w .

lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run

vet:
	@echo "Running go vet..."
	$(GOVET) ./...

install: build
	@echo "Installing $(BINARY_NAME)..."
	@cp $(BUILD_DIR)/$(BINARY_NAME) $(GOPATH)/bin/

help:
	@echo "Available targets:"
	@echo "  build        - Build the binary"
	@echo "  build-all    - Build for all platforms"
	@echo "  clean        - Clean build artifacts"
	@echo "  test         - Run tests"
	@echo "  test-coverage- Run tests with coverage"
	@echo "  deps         - Download dependencies"
	@echo "  deps-update  - Update dependencies"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linter"
	@echo "  vet          - Run go vet"
	@echo "  install      - Install binary to GOPATH"
	@echo "  web          - Build web UI"
	@echo "  web-dev      - Start web dev server"
	@echo "  server       - Build and start server with web UI"
	@echo "  server-dev   - Start server without rebuilding web UI"

