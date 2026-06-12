# Tell the Makefile these aren't real files, just commands
.PHONY: build build-docker run-local test deploy clean

# 1. Compile all Go microservices locally
build:
	go build -o fleet-binary cmd/fleet/main.go
	go build -o telemetry-binary cmd/telemetry/main.go
	go build -o leaderboard-binary cmd/leaderboard/main.go
	go build -o sandbox-binary cmd/sandbox/main.go
	go build -o mock-contestant-binary cmd/mock_contestant/main.go

# 2. Package everything into the Docker shipping container
build-docker:
	docker build -t iicpc-platform:latest .

# 3. Spin up the local databases and message queue
run-local:
	docker-compose up -d

# 4. Run all unit and integration tests
test:
	go test ./...

# 5. Hand the instruction manuals over to Kubernetes
deploy:
	kubectl apply -f k8s/

# 6. Tear down local services and delete compiled files
clean:
	rm -f *-binary
	docker-compose down