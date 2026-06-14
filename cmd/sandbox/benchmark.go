package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

func resolveTargetIP() string {
	if ip := os.Getenv("POD_IP"); ip != "" {
		return ip
	}
	if ip := os.Getenv("HOST_IP"); ip != "" {
		return ip
	}
	return "127.0.0.1"
}

type benchmarkConfig struct {
	URL       string
	NumBots   int
	NumOrders int
	TPS       int
}

func benchmarkDefaults() benchmarkConfig {
	return benchmarkConfig{
		URL:       getEnvOrDefault("FLEET_RUN_URL", "http://fleet:8080/run"),
		NumBots:   getEnvIntOrDefault("BENCH_NUM_BOTS", 200),
		NumOrders: getEnvIntOrDefault("BENCH_NUM_ORDERS", 10000),
		TPS:       getEnvIntOrDefault("BENCH_TPS", 500),
	}
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func triggerFleetBenchmark(ctx context.Context, submissionID, endpoint string) error {
	cfg := benchmarkDefaults()
	payload := map[string]any{
		"submission_id": submissionID,
		"endpoint":      endpoint,
		"num_bots":      cfg.NumBots,
		"num_orders":    cfg.NumOrders,
		"tps":           cfg.TPS,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal fleet request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create fleet request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("call fleet /run: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fleet /run failed with status %s", resp.Status)
	}

	return nil
}

func benchmarkTriggerTimeout() time.Duration {
	return 10 * time.Second
}
