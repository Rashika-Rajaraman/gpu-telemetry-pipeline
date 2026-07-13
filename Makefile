# Telemetry Pipeline — root Makefile
# Single Makefile with per-component targets (see README for rationale).

COMPONENTS := streamer messagequeue collector apigateway
MODULE     := github.com/gpu-telemetry-pipeline
REGISTRY   ?= localhost:5001
TAG        ?= dev

.DEFAULT_GOAL := help

## ---- Help ----------------------------------------------------------------
.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_/%-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

## ---- Build ---------------------------------------------------------------
.PHONY: build
build: ## Build all component binaries into ./bin
	@for c in $(COMPONENTS); do \
		echo ">> building $$c"; \
		go build -o bin/$$c ./$$c/cmd/... ; \
	done

build/%: ## Build one component, e.g. make build/streamer
	go build -o bin/$* ./$*/cmd/...

## ---- Test / Coverage -----------------------------------------------------
.PHONY: test
test: ## Run all unit tests
	go test ./... -count=1

test/%: ## Test one component, e.g. make test/collector
	go test ./$*/... -count=1 -cover

.PHONY: cover
cover: ## Run tests with aggregated coverage (summary + HTML)
	go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic
	@echo "----------------------------------------"
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out -o coverage.html
	@echo "HTML report: coverage.html"

.PHONY: lint
lint: ## Vet all packages
	go vet ./...

## ---- OpenAPI -------------------------------------------------------------
.PHONY: openapi
openapi: ## Generate api/openapi.yaml from the API gateway definitions
	go run ./apigateway/cmd --dump-openapi > api/openapi.yaml
	@echo "wrote api/openapi.yaml"

## ---- Docker --------------------------------------------------------------
.PHONY: docker
docker: ## Build all component images
	@for c in $(COMPONENTS); do \
		echo ">> image $$c"; \
		docker build -f $$c/Dockerfile -t $(REGISTRY)/$$c:$(TAG) . ; \
	done
	docker build -f database/Dockerfile -t $(REGISTRY)/database:$(TAG) . || true

docker/%: ## Build one image, e.g. make docker/broker (maps to messagequeue)
	docker build -f $*/Dockerfile -t $(REGISTRY)/$*:$(TAG) .

## ---- Kind / Helm ---------------------------------------------------------
.PHONY: kind-up
kind-up: ## Create local kind cluster + registry
	./scripts/kind-up.sh

.PHONY: images-load
images-load: ## Build & push all images into the kind registry
	./scripts/load-images.sh

.PHONY: helm-install
helm-install: ## Install all 5 charts into the cluster
	@for c in $(COMPONENTS) database; do \
		echo ">> helm install $$c"; \
		helm upgrade --install $$c deployment/helm/$$c --set image.repository=$(REGISTRY)/$$c --set image.tag=$(TAG) ; \
	done

.PHONY: helm-uninstall
helm-uninstall: ## Remove all charts
	@for c in $(COMPONENTS) database; do helm uninstall $$c || true ; done

## ---- Housekeeping --------------------------------------------------------
.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: clean
clean: ## Remove build + coverage artifacts
	rm -rf bin out coverage.out coverage.html
