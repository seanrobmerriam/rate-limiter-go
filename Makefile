.PHONY: test test-race vet lint bench run-demo docker-up docker-down

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run

bench:
	go test -bench=. -benchmem ./...

run-demo:
	go run ./cmd/demo

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down
