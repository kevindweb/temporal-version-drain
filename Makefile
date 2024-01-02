.PHONY: update
update:
	go get -u ./...
	go mod tidy

.PHONY: install
install:
	go install go.temporal.io/sdk/contrib/tools/workflowcheck@latest
	npm install -g prettier

.PHONY: lint
lint: check
	files=$(gofmt -l .) && [ -z "$(files)" ]
	golangci-lint run ./...

.PHONY: check
check:
	workflowcheck ./...
	prettier -l .

.PHONY: format
format:
	prettier -w .

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go clean -testcache
	go test -race ./...
