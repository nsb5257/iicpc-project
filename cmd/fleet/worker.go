package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"iicpc-platform/pkg/events"

	"github.com/segmentio/kafka-go"
)

func startLoadTest(req RunRequest) error {
	log.Printf("Starting load test %s for Submission %s against %s. Bots: %d, Orders: %d, TPS: %d",
		req.RunID, req.SubmissionID, req.Endpoint, req.NumBots, req.NumOrders, req.TPS)

	jobs := make(chan Order, req.NumOrders)
	tokens := make(chan time.Time, req.TPS) // Token bucket implementation

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Generate ticks perfectly matched to the requested TPS
	ticker := time.NewTicker(time.Second / time.Duration(req.TPS))
	defer ticker.Stop()

	go func() {
		for {
			select {
			case t := <-ticker.C:
				select {
				case tokens <- t:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	var publishFailures int32
	baseURL := fmt.Sprintf("http://%s", req.Endpoint)

	// Spawn requested number of bots
	for w := 1; w <= req.NumBots; w++ {
		wg.Add(1)
		go worker(w, req.RunID, req.SubmissionID, jobs, tokens, &wg, baseURL, &publishFailures)
	}

	// Feed diverse orders into the job queue
	for i := 1; i <= req.NumOrders; i++ {
		jobs <- generateOrder(i)
	}
	close(jobs)

	// Wait for all bots to finish processing
	wg.Wait()
	cancel()
	if atomic.LoadInt32(&publishFailures) > 0 {
		return fmt.Errorf("%d Kafka publish failures occurred during run %s", atomic.LoadInt32(&publishFailures), req.RunID)
	}
	log.Printf("Load test completed for Submission %s (run %s)", req.SubmissionID, req.RunID)
	return nil
}

func worker(id int, runID, submissionID string, jobs <-chan Order, tokens <-chan time.Time, wg *sync.WaitGroup, baseURL string, publishFailures *int32) {
	defer wg.Done()

	// Create a dedicated HTTP client per worker to enable efficient connection pooling
	client := &http.Client{Timeout: 5 * time.Second}

	for order := range jobs {
		<-tokens // Wait for a token to enforce global TPS

		jsonData, err := json.Marshal(order)
		if err != nil {
			log.Printf("[Bot %d] Failed to marshal order: %v", id, err)
			continue
		}

		// Route to /cancel for CANCEL orders, otherwise /order
		endpoint := "/order"
		if order.Type == "CANCEL" {
			endpoint = "/cancel"
		}
		targetURL := baseURL + endpoint

		httpReq, err := http.NewRequest("POST", targetURL, bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("[Bot %d] Failed to create request: %v", id, err)
			continue
		}
		httpReq.Header.Set("Content-Type", "application/json")

		sentTime := time.Now()
		resp, err := client.Do(httpReq)
		ackTime := time.Now()

		statusCode := 0
		isSuccess := false
		var responseBodyBytes []byte

		if err == nil {
			statusCode = resp.StatusCode
			// Determine initial success based on HTTP code
			isSuccess = (statusCode == 200 || statusCode == 201)

			// Protect memory: Read at most 2KB of the response body, discard the rest
			responseBodyBytes, _ = io.ReadAll(io.LimitReader(resp.Body, 2048))
			resp.Body.Close()
		}

		latency := ackTime.Sub(sentTime).Milliseconds()

		// Populate cross-file contract completely using shared package
		event := events.OrderEvent{
			RunID:              runID,
			OrderID:            order.ID,
			SubmissionID:       submissionID,
			Type:               order.Type,
			Timestamp:          order.Timestamp,
			SentAt:             sentTime.UnixNano(),
			AckAt:              ackTime.UnixNano(),
			LatencyMs:          latency,
			StatusCode:         statusCode,
			IsSuccessful:       isSuccess,
			ActualResponseBody: string(responseBodyBytes),
			ExpectedStatus:     200,
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			log.Printf("[Bot %d] Failed to marshal telemetry event: %v", id, err)
			continue
		}

		// Ship to Redpanda with Exponential Backoff
		retryDelay := 100 * time.Millisecond
		for attempt := 0; attempt < 3; attempt++ {
			err = kafkaWriter.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte(runID),
					Value: eventBytes,
				},
			)
			if err == nil {
				break // Success
			}
			log.Printf("[Bot %d] Kafka write failed (attempt %d/3): %v", id, attempt+1, err)
			if attempt < 2 {
				time.Sleep(retryDelay)
				retryDelay *= 2
			}
		}
		if err != nil {
			log.Printf("[Bot %d] final Kafka publish failure for order %s", id, order.ID)
			atomic.AddInt32(publishFailures, 1)
		}
	}
}
