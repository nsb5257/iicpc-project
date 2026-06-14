package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"regexp"

	pb "iicpc-platform/pb"

	"github.com/docker/docker/api/types/container"
)

var validIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// SandboxServer implements the generated gRPC interface
type SandboxServer struct {
	pb.UnimplementedSandboxServiceServer
	appCtx *AppContext
}

// getDeterministicPort hashes the submission ID to a stable port between 30000 and 40000
func getDeterministicPort(submissionID string) int {
	h := fnv.New32a()
	h.Write([]byte(submissionID))
	return int(30000 + (h.Sum32() % 10000))
}

// ExecuteSubmission acts as the gRPC wrapper for the internal logic
func (s *SandboxServer) ExecuteSubmission(ctx context.Context, req *pb.ExecuteRequest) (*pb.ExecuteResponse, error) {
	log.Printf("Received gRPC request to execute submission: %s (Language: %s)", req.SubmissionId, req.Language)

	if !validIDPattern.MatchString(req.SubmissionId) {
		defer os.Remove(req.SourceFilePath)
		return &pb.ExecuteResponse{Success: false, Message: "Invalid submission ID format"}, nil
	}

	language := req.Language
	if language == "" {
		language = "go"
	}

	ip, port, err := s.appCtx.executeSubmissionInternal(ctx, req.SubmissionId, req.SourceFilePath, language, "source")
	defer os.Remove(req.SourceFilePath)

	if err != nil {
		return &pb.ExecuteResponse{Success: false, Message: err.Error()}, nil
	}

	return &pb.ExecuteResponse{
		Success:       true,
		Message:       "Container is running",
		ContainerIp:   ip,
		ContainerPort: int32(port),
	}, nil
}

// executeSubmissionInternal handles the core logic for both HTTP and gRPC
func (app *AppContext) executeSubmissionInternal(ctx context.Context, submissionID, sourceFilePath, language, artifactType string) (string, int, error) {

	imageName := fmt.Sprintf("submission-%s", submissionID)

	// Pass artifactType as the 6th argument to buildSubmissionImage
	if err := buildSubmissionImage(ctx, app.DockerClient, sourceFilePath, imageName, language, artifactType); err != nil {
		return "", 0, fmt.Errorf("build failed: %v", err)
	}

	hostPort := getDeterministicPort(submissionID)
	targetIP := resolveTargetIP()

	containerID, err := runSubmissionContainer(ctx, app.DockerClient, submissionID, imageName, hostPort)
	if err != nil {
		return "", 0, fmt.Errorf("run failed: %v", err)
	}

	endpoint := fmt.Sprintf("%s:%d", targetIP, hostPort)
	if err := waitForReady(endpoint); err != nil {
		_ = app.DockerClient.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		return "", 0, fmt.Errorf("readiness check failed: %v", err)
	}

	return targetIP, hostPort, nil
}
