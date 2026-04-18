.PHONY: run test lint openapi-lint build docker-build up down

run:
	go run ./cmd/worker

test:
	go test ./...

openapi-lint:
	npx --yes @redocly/cli lint openapi/openapi.yaml

lint:
	go vet ./...
	$(MAKE) openapi-lint

build:
	go build -o bin/backtest-engine ./cmd/worker

docker-build:
	docker build -t backtest-engine:latest .

up:
	docker compose up -d

down:
	docker compose down
