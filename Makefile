.PHONY: test test-race test-short lint gofmt format setup bench examples

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

test-short:
	go test ./... -short -count=1

lint:
	go vet ./...

gofmt:
	test -z "$$(gofmt -s -l .)"

format:
	gofmt -s -w .

bench:
	go test -bench=. -benchmem ./push/

examples:
	go build -o bin/stock ./examples/stock

setup:
	@command -v pre-commit >/dev/null 2>&1 || (echo "install pre-commit: https://pre-commit.com" && exit 1)
	pre-commit install
