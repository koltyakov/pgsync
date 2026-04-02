#!/bin/bash

# Release script for pgsync
# Usage: ./scripts/release.sh v1.2.3

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if tag is provided
if [ $# -ne 1 ]; then
  echo -e "${RED}Error: Please provide a version tag${NC}"
  echo "Usage: $0 v1.2.3"
  exit 1
fi

TAG=$1

# Validate tag format
if [[ ! $TAG =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo -e "${RED}Error: Tag must be in format v1.2.3${NC}"
  exit 1
fi

echo -e "${GREEN}Preparing release $TAG${NC}"

# Check if we're on main branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$CURRENT_BRANCH" != "main" ]; then
  echo -e "${YELLOW}Warning: You're not on the main branch (current: $CURRENT_BRANCH)${NC}"
  read -p "Continue anyway? (y/N): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

# Check if working directory is clean
if [[ -n $(git status --porcelain) ]]; then
  echo -e "${RED}Error: Working directory is not clean${NC}"
  git status --short
  exit 1
fi

# Fetch latest changes
echo -e "${GREEN}Fetching latest changes...${NC}"
git fetch origin

# Check if tag already exists
if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo -e "${RED}Error: Tag $TAG already exists${NC}"
  exit 1
fi

# Run tests
echo -e "${GREEN}Running tests...${NC}"
go test ./...

# Build to ensure everything compiles
echo -e "${GREEN}Building...${NC}"
go build ./...

# Create and push tag
echo -e "${GREEN}Creating and pushing tag $TAG...${NC}"
git tag -a "$TAG" -m "Release $TAG"
git push origin "$TAG"

echo -e "${GREEN}Release $TAG has been tagged and pushed!${NC}"
echo -e "${GREEN}GitHub Actions will now build and publish the release.${NC}"
echo -e "${GREEN}Check the progress at: https://github.com/koltyakov/pgsync/actions${NC}"
