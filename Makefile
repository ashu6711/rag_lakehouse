# Use .PHONY to ensure these targets run even if files with the same name exist.
.PHONY: all build up down restart logs ps

# Default target that runs when you just type 'make'.
# It will first build the images and then start the services.
all: build up

build:
	@echo "Building all Docker images..."
	docker-compose build --no-cache

# Start all services defined in docker-compose.yaml in detached mode (-d).
up:
	@echo "Starting all services in the background..."
	docker-compose up -d

# Stop and remove all containers, networks, and volumes created by 'up'.
down:
	@echo "Stopping and removing all containers and networks..."
	docker-compose down

# A convenient shortcut to restart all services.
restart: down up

# List all running containers and their status.
ps:
	@echo "Listing all running containers..."
	docker-compose ps

