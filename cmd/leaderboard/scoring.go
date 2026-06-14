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

type LeaderboardStats struct {
	RunID           string  `json:"run_id"`
	SubmissionID    string  `json:"submission_id"`
	TPS             float64 `json:"tps"`
	P50Latency      float64 `json:"p50_latency_ms"`
	P90Latency      float64 `json:"p90_latency_ms"`
	P99Latency      float64 `json:"p99_latency_ms"`
	ErrorRate       float64 `json:"error_rate"`
	CorrectnessRate float64 `json:"correctness_rate"`
	Score           float64 `json:"composite_score"`
}

func runScoringEngine(db *sql.DB, rdb *redis.Client) {
	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	query := `
		WITH windowed AS (
			SELECT
				run_id,
				submission_id,
				latency_ms,
				is_successful,
				is_correct,
				time
			FROM order_metrics
			WHERE time > NOW() - INTERVAL '15 minutes'
		),
		aggregated AS (
			SELECT
				run_id,
				submission_id,
				COUNT(*) AS total_count,
				SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS success_count,
				SUM(CASE WHEN is_correct    THEN 1 ELSE 0 END) AS correct_count,
				COALESCE(
					SUM(CASE WHEN is_successful AND is_correct THEN 1 ELSE 0 END)
					/ NULLIF(EXTRACT(EPOCH FROM (MAX(time) - MIN(time))), 0),
					0
				) AS tps,
				COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY CASE WHEN is_successful AND is_correct THEN latency_ms END), 0) AS p50_latency,
				COALESCE(percentile_cont(0.90) WITHIN GROUP (ORDER BY CASE WHEN is_successful AND is_correct THEN latency_ms END), 0) AS p90_latency,
				COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY CASE WHEN is_successful AND is_correct THEN latency_ms END), 0) AS p99_latency
			FROM windowed
			GROUP BY run_id, submission_id
			HAVING COUNT(*) > 1
		)
		SELECT
			run_id, submission_id, tps,
			p50_latency, p90_latency, p99_latency,
			total_count, success_count, correct_count
		FROM aggregated;`

	for range ticker.C {
		// ── Leader election: only one replica scores per tick ─────────────
		acquired, err := rdb.SetNX(ctx, "scorer:leader-lock", "1", 5*time.Second).Result()
		if err != nil {
			log.Printf("Redis SetNX error (skipping tick): %v", err)
			continue
		}
		if !acquired {
			continue // Another replica holds the lock; skip this tick.
		}
		// ─── Only the leader reaches here ────────────────────────────────

		rows, err := db.Query(query)
		if err != nil {
			log.Printf("Failed to execute scoring query: %v", err)
			continue
		}

		var currentBoard []LeaderboardStats

		for rows.Next() {
			var stats LeaderboardStats
			var totalCount, successCount, correctCount int64
			if err := rows.Scan(
				&stats.RunID,
				&stats.SubmissionID,
				&stats.TPS,
				&stats.P50Latency,
				&stats.P90Latency,
				&stats.P99Latency,
				&totalCount,
				&successCount,
				&correctCount,
			); err != nil {
				log.Printf("Row scan error: %v", err)
				continue
			}

			if totalCount > 0 {
				stats.ErrorRate = 1.0 - float64(successCount)/float64(totalCount)
			} else {
				stats.ErrorRate = 1.0
			}

			if successCount > 0 {
				stats.CorrectnessRate = float64(correctCount) / float64(successCount)
			} else {
				stats.CorrectnessRate = 0.0
			}

			effectiveP90 := stats.P90Latency
			if effectiveP90 < 1 {
				effectiveP90 = 1
			}

			stats.Score = (stats.TPS * 100 / effectiveP90) * (1 - stats.ErrorRate) * stats.CorrectnessRate

			currentBoard = append(currentBoard, stats)
		}
		rows.Close()

		if len(currentBoard) == 0 {
			continue
		}

		sort.Slice(currentBoard, func(i, j int) bool {
			return currentBoard[i].Score > currentBoard[j].Score
		})

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
