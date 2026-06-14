package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"google.golang.org/grpc"

	pb "iicpc-platform/pb"
)

// AppContext holds shared dependencies like the Docker client
type AppContext struct {
	DockerClient *client.Client
}

// ensureSandboxNetwork creates an isolated bridge network with no internet egress
func ensureSandboxNetwork(ctx context.Context, cli *client.Client) error {
	networks, err := cli.NetworkList(ctx, network.ListOptions{})
	if err != nil {
		return err
	}

	for _, nw := range networks {
		if nw.Name == "iicpc-sandbox-net" {
			return nil // Network already exists
		}
	}

	log.Println("Creating internal Docker network 'iicpc-sandbox-net'...")
	_, err = cli.NetworkCreate(ctx, "iicpc-sandbox-net", network.CreateOptions{
		Internal: true, // Crucial: Drops all internet egress traffic
	})
	return err
}

func main() {
	log.Println("Starting Sandboxing Engine...")

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Failed to initialize Docker client: %v", err)
	}
	defer cli.Close()

	ping, err := cli.Ping(context.Background())
	if err != nil {
		log.Fatalf("Cannot connect to Docker daemon: %v", err)
	}
	log.Printf("Successfully connected to Docker API version: %s", ping.APIVersion)

	// Enforce network isolation topology
	if err := ensureSandboxNetwork(context.Background(), cli); err != nil {
		log.Fatalf("Failed to establish isolated sandbox network: %v", err)
	}

	app := &AppContext{DockerClient: cli}

	// Start background janitor
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Println("Running scheduled Docker image cleanup...")
				if err := cleanupOldImages(context.Background(), cli); err != nil {
					log.Printf("Cleanup error: %v", err)
				}
			case <-cleanupCtx.Done():
				return
			}
		}
	}()

	// Start HTTP Server
	http.HandleFunc("/upload", app.uploadHandler)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if _, err := cli.Ping(context.Background()); err != nil {
			http.Error(w, `{"error":"docker_down"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})

	httpServer := &http.Server{Addr: ":8081"}
	go func() {
		log.Println("HTTP Server listening on :8081 (for file uploads & health)...")
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP Server failed: %v", err)
		}
	}()

	// Start gRPC Server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSandboxServiceServer(grpcServer, &SandboxServer{appCtx: app})

	// Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Shutting down gracefully...", sig)
		cleanupCancel()
		httpServer.Shutdown(context.Background())
		grpcServer.GracefulStop()
	}()

	log.Println("gRPC Server listening on :50051 (for Bot Fleet commands)...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC Server failed: %v", err)
	}
}
