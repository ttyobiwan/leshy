SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

.DEFAULT_GOAL := help
.PHONY: help
help: ## Display this help section
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z\$$/]+.*:.*?##\s/ {printf "\033[36m%-38s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: server
server: ## Start gRPC server
	go run cmd/server/main.go

.PHONY: listener 
listener: ## Start listener
	go run cmd/listener/main.go

.PHONY: publisher 
publisher: ## Send gRPC message
	go run cmd/publisher/main.go
