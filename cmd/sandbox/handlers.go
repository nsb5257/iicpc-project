package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// uploadHandler processes multipart form uploads for bot submissions
func (app *AppContext) uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseMultipartForm(10 << 20); err != nil { // 10 MB limit
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	submissionID := r.FormValue("submission_id")
	if submissionID == "" {
		http.Error(w, "submission_id is required", http.StatusBadRequest)
		return
	}
	language := r.FormValue("language")
	if language == "" {
		language = "go"
	}

	file, _, err := r.FormFile("source_file")
	if err != nil {
		http.Error(w, "source_file is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Save file temporarily using a unique path to avoid concurrent upload races
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("submission-%s-*", submissionID))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpPath := tmpFile.Name()
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	if _, err := io.Copy(tmpFile, file); err != nil {
		http.Error(w, fmt.Sprintf("failed to store upload: %v", err), http.StatusInternalServerError)
		return
	}
	if err := tmpFile.Close(); err != nil {
		http.Error(w, fmt.Sprintf("failed to close upload file: %v", err), http.StatusInternalServerError)
		return
	}

	// Execute via internal engine
	ip, port, err := app.executeSubmissionInternal(r.Context(), submissionID, tmpPath, language)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	endpoint := fmt.Sprintf("%s:%d", ip, port)
	benchCtx, cancel := context.WithTimeout(context.Background(), benchmarkTriggerTimeout())
	defer cancel()
	if err := triggerFleetBenchmark(benchCtx, submissionID, endpoint); err != nil {
		http.Error(w, fmt.Sprintf("container deployed but failed to start benchmark: %v", err), http.StatusBadGateway)
		return
	}

	resp := map[string]interface{}{
		"success":        true,
		"message":        "Container is running",
		"container_ip":   ip,
		"container_port": port,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
