// sandbox/main.go
package main

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	// Replace "iicpc-platform" with your actual go.mod module name if it is different!
	pb "iicpc-platform/pb"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// This regex ensures submission IDs only contain safe letters and numbers
var validIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// uploadHandler processes incoming file uploads
func uploadHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// 2. Parse the incoming multipart form (limit max memory to 10MB)
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	// 3. Retrieve the file from the form data
	file, handler, err := r.FormFile("sourcecode")
	if err != nil {
		http.Error(w, "Error retrieving file from request", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// 4. Create a temporary file safely
	tempFile, err := os.CreateTemp("", "submission-*.go")
	if err != nil {
		http.Error(w, "Error creating temporary file", http.StatusInternalServerError)
		return
	}
	defer tempFile.Close()

	// 5. Copy the uploaded file's contents to the temporary file
	_, err = io.Copy(tempFile, file)
	if err != nil {
		http.Error(w, "Error saving file", http.StatusInternalServerError)
		return
	}

	// 6. Respond with success
	fmt.Fprintf(w, "Successfully uploaded %s to %s\n", handler.Filename, tempFile.Name())
	log.Printf("Received and saved submission: %s\n", tempFile.Name())
}

// buildSubmissionImage packages the user's code and a Dockerfile into a tarball,
// then sends it to the Docker daemon to build an isolated image.
func buildSubmissionImage(ctx context.Context, cli *client.Client, sourceFilePath string, imageName string) error {
	log.Printf("Building Docker image '%s' for %s...", imageName, sourceFilePath)

	// 1. Read the contestant's uploaded code
	codeBytes, err := os.ReadFile(sourceFilePath)
	if err != nil {
		return fmt.Errorf("failed to read source file: %v", err)
	}

	// 2. Define a minimal Dockerfile to compile and run their Go code
	dockerfile := `
FROM golang:1.22-alpine
WORKDIR /app
COPY main.go .
RUN go build -o bot main.go
CMD ["./bot"]
`
	// 3. Create a new TAR archive in memory
	var buf bytes.Buffer
	tarWriter := tar.NewWriter(&buf)

	// Helper function to write files into our tarball
	addFileToTar := func(name, body string) error {
		header := &tar.Header{
			Name: name,
			Size: int64(len(body)),
			Mode: 0600, // Read/write permissions
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		_, err := tarWriter.Write([]byte(body))
		return err
	}

	// 4. Add the Dockerfile and the contestant's code to the TAR archive
	_ = addFileToTar("Dockerfile", dockerfile)
	_ = addFileToTar("main.go", string(codeBytes))
	tarWriter.Close() // Flush the writer

	// 5. Send the TAR archive to the Docker daemon to build the image
	buildOptions := types.ImageBuildOptions{
		Tags:   []string{imageName},
		Remove: true, // Clean up intermediate containers
	}

	buildResponse, err := cli.ImageBuild(ctx, &buf, buildOptions)
	if err != nil {
		return fmt.Errorf("docker build failed: %v", err)
	}
	defer buildResponse.Body.Close()

	// 6. Print the build output to our console so we can see what Docker is doing
	_, err = io.Copy(os.Stdout, buildResponse.Body)
	if err != nil {
		return err
	}

	log.Printf("Successfully built image: %s", imageName)
	return nil
}

// runSubmissionContainer creates and starts an isolated container with strict resource limits
func runSubmissionContainer(ctx context.Context, cli *client.Client, imageName string) error {
	log.Printf("Spawning sandboxed container from image '%s'...", imageName)

	// 1. Configure what the container RUNS (Config)
	containerConfig := &container.Config{
		Image: imageName,
		// Expose port 8080 inside the container so our bots can talk to it later
		ExposedPorts: nat.PortSet{
			"8080/tcp": struct{}{},
		},
	}

	// 2. Configure how the container is ISOLATED on the host (HostConfig)
	hostConfig := &container.HostConfig{
		AutoRemove: true, // Automatically clean up the container when it exits
		Resources: container.Resources{
			// Limit memory to exactly 128 MB (128 * 1024 * 1024 bytes)
			Memory: 128 << 20,
			// CPU Pinning: Force this container to ONLY run on CPU core 0
			CpusetCpus: "0",
		},
		// Map the container's port 8080 to a random available port on our WSL2 machine
		PublishAllPorts: true,
	}

	// 3. Create the container (but don't start it yet)
	resp, err := cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		return fmt.Errorf("failed to create container: %v", err)
	}

	// 4. Start the container!
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %v", err)
	}

	log.Printf("Successfully started sandboxed container! ID: %s", resp.ID[:10])
	return nil
}

// cleanupOldImages acts as a janitor to prevent our hard drive from filling up.
func cleanupOldImages(ctx context.Context, cli *client.Client) error {
	images, err := cli.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return err
	}
	for _, img := range images {
		for _, tag := range img.RepoTags {
			if strings.HasPrefix(tag, "submission-") {
				// Remove images older than 1 hour
				created := time.Unix(img.Created, 0)
				if time.Since(created) > 1*time.Hour {
					_, err := cli.ImageRemove(ctx, tag, image.RemoveOptions{Force: true})
					if err != nil {
						log.Printf("Failed to remove old image %s: %v", tag, err)
					}
				}
			}
		}
	}
	return nil
}

// SandboxServer implements the generated gRPC interface
type SandboxServer struct {
	pb.UnimplementedSandboxServiceServer                // Required by gRPC for forward-compatibility
	DockerClient                         *client.Client // We store the Docker client here so our methods can use it
}

// ExecuteSubmission is triggered over the network by the Bot Fleet
func (s *SandboxServer) ExecuteSubmission(ctx context.Context, req *pb.ExecuteRequest) (*pb.ExecuteResponse, error) {
	log.Printf("Received gRPC request to execute submission: %s", req.SubmissionId)

	// SECURITY: Ensure the submission ID is safe to use as an image name
	if !validIDPattern.MatchString(req.SubmissionId) {
		defer os.Remove(req.SourceFilePath) // Always clean up the file
		return &pb.ExecuteResponse{
			Success: false,
			Message: "Invalid submission ID format",
		}, nil
	}

	imageName := fmt.Sprintf("submission-%s", req.SubmissionId)

	// 1. Build the image
	err := buildSubmissionImage(ctx, s.DockerClient, req.SourceFilePath, imageName)
	defer os.Remove(req.SourceFilePath)
	if err != nil {
		return &pb.ExecuteResponse{
			Success: false,
			Message: fmt.Sprintf("Build failed: %v", err),
		}, nil
	}

	// 2. Run the container
	err = runSubmissionContainer(ctx, s.DockerClient, imageName)
	if err != nil {
		return &pb.ExecuteResponse{
			Success: false,
			Message: fmt.Sprintf("Run failed: %v", err),
		}, nil
	}

	// 3. Dynamically fetch the actual port Docker assigned to this container
	containers, err := s.DockerClient.ContainerList(ctx, container.ListOptions{Latest: true})
	if err != nil || len(containers) == 0 {
		return &pb.ExecuteResponse{
			Success: false,
			Message: "Failed to locate newly started container",
		}, nil
	}
	
	// Inspect the newest container to find its port mapping
	inspect, err := s.DockerClient.ContainerInspect(ctx, containers[0].ID)
	if err != nil {
		return &pb.ExecuteResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to inspect container: %v", err),
		}, nil
	}

	containerPort := nat.Port("8080/tcp")
	bindings, exists := inspect.NetworkSettings.Ports[containerPort]
	if !exists || len(bindings) == 0 {
		return &pb.ExecuteResponse{
			Success: false,
			Message: "No port binding found for container",
		}, nil
	}

	hostPort := bindings[0].HostPort
	hostIP := bindings[0].HostIP
	
	// In Kubernetes/Docker setups, 0.0.0.0 means it's listening on all network interfaces. 
	// The Bot fleet should connect to it via the host's IP or loopback.
	if hostIP == "" || hostIP == "0.0.0.0" {
		hostIP = "127.0.0.1"
	}

	portInt, err := strconv.ParseInt(hostPort, 10, 32)
	if err != nil {
		return &pb.ExecuteResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to parse port: %v", err),
		}, nil
	}

	return &pb.ExecuteResponse{
		Success:       true,
		Message:       "Container is running",
		ContainerIp:   hostIP,
		ContainerPort: int32(portInt),
	}, nil
}

func main() {
	// --- 1. DOCKER INITIALIZATION ---
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

	// --- 2. START BACKGROUND JANITOR ---
	// This ticker wakes up every 30 minutes and cleans old images
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		for range ticker.C {
			log.Println("Running scheduled Docker image cleanup...")
			if err := cleanupOldImages(context.Background(), cli); err != nil {
				log.Printf("Cleanup error: %v", err)
			}
		}
	}()

	// --- 3. START HTTP SERVER (IN A GOROUTINE) ---
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		_, err := cli.Ping(context.Background())
		if err != nil {
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

	// --- 4. START gRPC SERVER ---
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	myServer := &SandboxServer{DockerClient: cli}
	pb.RegisterSandboxServiceServer(grpcServer, myServer)

	// --- 5. GRACEFUL SHUTDOWN HANDLER ---
	// This listens for Kubernetes telling us to shut down
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Shutting down gracefully...", sig)
		
		// Politely ask both servers to finish what they are doing and stop
		httpServer.Shutdown(context.Background())
		grpcServer.GracefulStop()
	}()

	// Start serving gRPC! (This blocks the main thread so the program doesn't exit)
	log.Println("gRPC Server listening on :50051 (for Bot Fleet commands)...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC Server failed: %v", err)
	}
}
