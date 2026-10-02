SHELL=/bin/bash

include .versions.env
export

GOPATH_BIN := $(shell go env GOBIN)
ifeq ($(strip $(GOPATH_BIN)),)
GOPATH_BIN := $(shell go env GOPATH)/bin
endif

PKG := ./...
DEFAULT_TESTS_TIMEOUT := 1m

GIT_REMOTE ?= $(shell git remote | grep -x upstream || git remote | grep -x origin || git remote | head -1)
BASE_BRANCH ?= $(shell git remote show $(GIT_REMOTE) 2>/dev/null | sed -n '/HEAD branch/s/.*: //p')
BASE_REF ?= $(if $(BASE_BRANCH),$(GIT_REMOTE)/$(BASE_BRANCH),upstream/main)

.PHONY: help build test fmt lint lint-fix lint-new-issues deps-setup-lint notices licenses-check

help:
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s : %s\n", $$1, $$2}'

build: ## Build the streamio CLI for the host OS/arch.
	go build -o build/streamio ./cmd/streamio

test: ## Run unit tests.
	go test -v `go list $(PKG)` -race -covermode=atomic -count=1 -shuffle=on -timeout $(DEFAULT_TESTS_TIMEOUT)

deps-setup-lint: ## Install golangci-lint. Used standalone by CI.
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/main/install.sh | \
		sh -s -- -b $(GOPATH_BIN) $(GOLANGCI_LINT_VERSION)

lint: ## Run linters on Go code.
	$(GOPATH_BIN)/golangci-lint run -v

lint-fix: ## Run linters and perform fixes. Preferably used in local development.
	$(GOPATH_BIN)/golangci-lint run --fix && \
	$(GOPATH_BIN)/golangci-lint run -v

lint-new-issues: ## Run linters checking only for new issues introduced compared to $(BASE_REF) (default: autodetected remote default branch). Override with `make lint-new-issues BASE_REF=<ref>`.
	$(GOPATH_BIN)/golangci-lint run --new-from-merge-base=$(BASE_REF)

fmt: ## Format Go code.
	$(GOPATH_BIN)/golangci-lint fmt

notices: ## Generate THIRD_PARTY_NOTICES.md (done automatically on release).
	scripts/third-party-notices.sh

licenses-check: ## Fail if any dependency uses a non-permissive license.
	go run github.com/google/go-licenses/v2@$(GO_LICENSES_VERSION) check ./cmd/streamio --allowed_licenses=MIT,BSD-3-Clause,BSD-2-Clause,Apache-2.0,ISC 2>&1 | grep -vE 'non-Go code|\.(s|h)$$' ; test $${PIPESTATUS[0]} -eq 0
