# Stage 1: Build the Go binaries
FROM golang:1.26-alpine AS builder
WORKDIR /app

# Copy dependency files and download them
COPY go.mod go.sum ./
RUN go mod download

# Copy your actual source code
COPY . .

# Compile all microservices
RUN go build -o fleet-binary cmd/fleet/main.go
RUN go build -o telemetry-binary cmd/telemetry/main.go
RUN go build -o leaderboard-binary cmd/leaderboard/main.go
RUN go build -o sandbox-binary cmd/sandbox/main.go
RUN go build -o mock-contestant-binary cmd/mock_contestant/main.go

# Stage 2: Create a tiny production image
FROM alpine:latest
WORKDIR /root/

# Copy the compiled binaries from Stage 1
COPY --from=builder /app/fleet-binary .
COPY --from=builder /app/telemetry-binary .
COPY --from=builder /app/leaderboard-binary .
COPY --from=builder /app/sandbox-binary .
COPY --from=builder /app/mock-contestant-binary .

# Copy the HTML file required for the web server
COPY index.html .

# Install ca-certificates for secure HTTPS requests
RUN apk --no-cache add ca-certificates