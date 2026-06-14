package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// LeaderboardStats matches the contract and UI expectations
type LeaderboardStats struct {
	RunID        string  `json:"run_id"`
	SubmissionID string  `json:"submission_id"`
	TPS          float64 `json:"tps"`
	P50Latency   float64 `json:"p50_latency_ms"`
	P90Latency   float64 `json:"p90_latency_ms"`
	P99Latency   float64 `json:"p99_latency_ms"`
	Score        float64 `json:"composite_score"`
}

func runScoringEngine(db *sql.DB, rdb *redis.Client) {
	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Aggregates over a rolling 15-minute window with division-by-zero protection
	query := `
		SELECT 
			run_id,
			submission_id,
			COALESCE(COUNT(*) / NULLIF(EXTRACT(EPOCH FROM (MAX(time) - MIN(time))), 0), 0) as tps,
			COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms), 0) as p50_latency,
			COALESCE(percentile_cont(0.90) WITHIN GROUP (ORDER BY latency_ms), 0) as p90_latency,
			COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms), 0) as p99_latency
		FROM order_metrics
		WHERE is_successful = TRUE 
		  AND is_correct = TRUE
		  AND time > NOW() - INTERVAL '15 minutes'
		GROUP BY run_id, submission_id
		HAVING COUNT(*) > 1;`

	for range ticker.C {
		rows, err := db.Query(query)
		if err != nil {
			log.Printf("Failed to execute scoring query: %v", err)
			continue
		}

		var currentBoard []LeaderboardStats

		for rows.Next() {
			var stats LeaderboardStats
			if err := rows.Scan(
				&stats.RunID,
				&stats.SubmissionID,
				&stats.TPS,
				&stats.P50Latency,
				&stats.P90Latency,
				&stats.P99Latency,
			); err != nil {
				log.Printf("Row scan error: %v", err)
				continue
			}

			// Calculate composite score: higher TPS is better, lower latency is better
			effectiveP90 := stats.P90Latency
			if effectiveP90 < 1 {
				effectiveP90 = 1
			}
			stats.Score = (stats.TPS * 100) / effectiveP90

			currentBoard = append(currentBoard, stats)
		}
		rows.Close()

		// If no submissions exist yet, skip broadcasting to avoid UI flashes
		if len(currentBoard) == 0 {
			continue
		}

		// Rank the array descending by composite score
		sort.Slice(currentBoard, func(i, j int) bool {
			return currentBoard[i].Score > currentBoard[j].Score
		})

		// Broadcast ranked array to Redis
		jsonData, err := json.Marshal(currentBoard)
		if err != nil {
			log.Printf("Failed to marshal leaderboard: %v", err)
			continue
		}

		if err := rdb.Publish(ctx, "live-scores", jsonData).Err(); err != nil {
			log.Printf("Redis publish error: %v", err)
		}
	}
}
