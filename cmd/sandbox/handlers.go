package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
	language := r.FormValue("language")
	if language == "" {
		language = "go"
	}

	file, header, err := r.FormFile("source_file")
	if err != nil {
		http.Error(w, "source_file is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Save file temporarily
	tmpPath := filepath.Join(os.TempDir(), header.Filename)
	out, err := os.Create(tmpPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer out.Close()
	io.Copy(out, file)

	// Execute via internal engine
	ip, port, err := app.executeSubmissionInternal(r.Context(), submissionID, tmpPath, language)

	// Clean up source file regardless of execution success
	os.Remove(tmpPath)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Dynamic IP resolution for Kubernetes environments
	nodeIP := os.Getenv("NODE_IP")
	if nodeIP == "" {
		nodeIP = ip // Fallback to 127.0.0.1 for local docker-compose
	}

	resp := map[string]interface{}{
		"success":        true,
		"message":        "Container is running",
		"container_ip":   nodeIP,
		"container_port": port,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
