package events

// OrderEvent strictly enforces the cross-service contract via Kafka
type OrderEvent struct {
	OrderID            string `json:"order_id"`
	SubmissionID       string `json:"submission_id"`
	Type               string `json:"type"`      // LIMIT, MARKET, CANCEL
	Timestamp          int64  `json:"timestamp"` // Original fleet generation time
	SentAt             int64  `json:"sent_at"`
	AckAt              int64  `json:"ack_at"`
	LatencyMs          int64  `json:"latency_ms"`
	StatusCode         int    `json:"status_code"`
	IsSuccessful       bool   `json:"is_successful"`
	ActualResponseBody string `json:"actual_response_body"`
	ExpectedStatus     int    `json:"expected_status"`
}
