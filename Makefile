.PHONY: update
update:
	go get -u ./...
	go mod tidy

.PHONY: lint
lint:
	files=$(gofmt -l .) && [ -z "$(files)" ]
	golangci-lint run ./...

.PHONY: test
test:
	go clean -testcache
	go test -race ./...
