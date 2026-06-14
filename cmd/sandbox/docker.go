package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// waitForReady probes the given TCP endpoint until it responds or times out.
func waitForReady(endpoint string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", endpoint, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("container at %s not ready after 10s", endpoint)
}

// buildSubmissionImage packages the user's code and specific Dockerfile into a tarball
func buildSubmissionImage(ctx context.Context, cli *client.Client, sourceFilePath, imageName, language string) error {
	log.Printf("Building Docker image '%s' for %s (lang: %s)...", imageName, sourceFilePath, language)

	codeBytes, err := os.ReadFile(sourceFilePath)
	if err != nil {
		return fmt.Errorf("failed to read source file: %v", err)
	}

	var dockerfile string
	var sourceFilename string

	switch strings.ToLower(language) {
	case "cpp":
		sourceFilename = "main.cpp"
		dockerfile = `
FROM gcc:13
WORKDIR /app
COPY main.cpp .
RUN g++ -O3 -o bot main.cpp
CMD ["./bot"]
`
	case "rust":
		sourceFilename = "main.rs"
		dockerfile = `
FROM rust:1.77-alpine
WORKDIR /app
COPY main.rs .
RUN rustc -O -o bot main.rs
CMD ["./bot"]
`
	default: // "go"
		sourceFilename = "main.go"
		dockerfile = `
FROM golang:1.22-alpine
WORKDIR /app
COPY main.go .
RUN go build -o bot main.go
CMD ["./bot"]
`
	}

	var buf bytes.Buffer
	tarWriter := tar.NewWriter(&buf)

	addFileToTar := func(name, body string) error {
		header := &tar.Header{
			Name: name,
			Size: int64(len(body)),
			Mode: 0600,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		_, err := tarWriter.Write([]byte(body))
		return err
	}

	_ = addFileToTar("Dockerfile", dockerfile)
	_ = addFileToTar(sourceFilename, string(codeBytes))
	tarWriter.Close()

	// Remove any previous image tag so failed rebuilds cannot accidentally reuse stale images.
	_, _ = cli.ImageRemove(ctx, imageName, image.RemoveOptions{Force: true, PruneChildren: true})

	buildOptions := types.ImageBuildOptions{
		Tags:   []string{imageName},
		Remove: true,
	}

	buildResponse, err := cli.ImageBuild(ctx, &buf, buildOptions)
	if err != nil {
		return fmt.Errorf("docker build failed: %v", err)
	}
	defer buildResponse.Body.Close()

	type buildMessage struct {
		Stream      string `json:"stream"`
		Error       string `json:"error"`
		ErrorDetail struct {
			Message string `json:"message"`
		} `json:"errorDetail"`
	}

	decoder := json.NewDecoder(buildResponse.Body)
	for {
		var msg buildMessage
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if msg.Stream != "" {
			fmt.Print(msg.Stream)
		}
		if msg.Error != "" {
			return fmt.Errorf("docker build failed: %s", msg.Error)
		}
		if msg.ErrorDetail.Message != "" {
			return fmt.Errorf("docker build failed: %s", msg.ErrorDetail.Message)
		}
	}

	return nil
}

// runSubmissionContainer creates and starts a container mapped to the deterministic port
func runSubmissionContainer(ctx context.Context, cli *client.Client, submissionID, imageName string, hostPort int) (string, error) {
	containerName := fmt.Sprintf("submission-%s", submissionID)
	_ = cli.ContainerRemove(ctx, containerName, container.RemoveOptions{Force: true, RemoveVolumes: true})

	containerConfig := &container.Config{
		Image: imageName,
		ExposedPorts: nat.PortSet{
			"8080/tcp": struct{}{},
		},
	}

	portStr := strconv.Itoa(hostPort)

	// Dynamic CPU pinning to distribute contestant load across available cores
	numCPUs := runtime.NumCPU()
	cpuCore := strconv.Itoa(hostPort % numCPUs)

	hostConfig := &container.HostConfig{
		AutoRemove: true,
		Resources: container.Resources{
			Memory:     128 << 20,
			CpusetCpus: cpuCore,
		},
		// Explicitly bind to our deterministic port
		PortBindings: nat.PortMap{
			"8080/tcp": []nat.PortBinding{
				{HostIP: "0.0.0.0", HostPort: portStr},
			},
		},
	}

	resp, err := cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil {
		return "", fmt.Errorf("failed to create container: %v", err)
	}

	cleanup := func(reason error) (string, error) {
		_ = cli.ContainerStop(ctx, resp.ID, container.StopOptions{})
		_ = cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		return "", reason
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return cleanup(fmt.Errorf("failed to start container: %v", err))
	}

	// Connect running container to the internal isolated network
	err = cli.NetworkConnect(ctx, "iicpc-sandbox-net", resp.ID, &network.EndpointSettings{})
	if err != nil {
		return cleanup(fmt.Errorf("failed to connect container to internal network: %v", err))
	}

	return resp.ID, nil
}

func cleanupOldImages(ctx context.Context, cli *client.Client) error {
	images, err := cli.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return err
	}
	for _, img := range images {
		for _, tag := range img.RepoTags {
			if strings.HasPrefix(tag, "submission-") {
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
