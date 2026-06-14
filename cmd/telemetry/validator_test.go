package main

import (
	"encoding/json"
	"testing"
	"time"

	"iicpc-platform/pkg/events"
)

// makeJSON is a test helper to easily mock the contestant container's HTTP response
func makeJSON(resp TargetResponse) string {
	b, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestValidateOrder(t *testing.T) {
	tests := []struct {
		name     string
		event    events.OrderEvent
		expected bool
	}{
		{
			name: "1. Valid LIMIT BUY fills at or below limit price",
			event: events.OrderEvent{
				OrderID:        "ORD-1",
				SubmissionID:   "SUB-TEST",
				Type:           "LIMIT",
				Timestamp:      time.Now().UnixNano(),
				StatusCode:     200,
				ExpectedStatus: 200,
				IsSuccessful:   true,
				ActualResponseBody: makeJSON(TargetResponse{
					OrderID:        "ORD-1",
					Type:           "LIMIT",
					Side:           "BUY",
					OrderedQty:     10,
					FilledQty:      10,
					Price:          100.00,
					ExecutionPrice: 99.50, // Better than limit
				}),
			},
			expected: true,
		},
		{
			name: "2. Valid LIMIT SELL fills at or above limit price",
			event: events.OrderEvent{
				OrderID:        "ORD-2",
				SubmissionID:   "SUB-TEST",
				Type:           "LIMIT",
				Timestamp:      time.Now().UnixNano(),
				StatusCode:     200,
				ExpectedStatus: 200,
				IsSuccessful:   true,
				ActualResponseBody: makeJSON(TargetResponse{
					OrderID:        "ORD-2",
					Type:           "LIMIT",
					Side:           "SELL",
					OrderedQty:     10,
					FilledQty:      10,
					Price:          100.00,
					ExecutionPrice: 100.50, // Better than limit
				}),
			},
			expected: true,
		},
		{
			name: "3. LIMIT BUY fills above limit price -> incorrect",
			event: events.OrderEvent{
				OrderID:        "ORD-3",
				SubmissionID:   "SUB-TEST",
				Type:           "LIMIT",
				Timestamp:      time.Now().UnixNano(),
				StatusCode:     200,
				ExpectedStatus: 200,
				IsSuccessful:   true,
				ActualResponseBody: makeJSON(TargetResponse{
					OrderID:        "ORD-3",
					Type:           "LIMIT",
					Side:           "BUY",
					OrderedQty:     10,
					FilledQty:      10,
					Price:          100.00,
					ExecutionPrice: 101.00, // Worse than limit (Violation)
				}),
			},
			expected: false,
		},
		{
			name: "4. LIMIT SELL fills below limit price -> incorrect",
			event: events.OrderEvent{
				OrderID:        "ORD-4",
				SubmissionID:   "SUB-TEST",
				Type:           "LIMIT",
				Timestamp:      time.Now().UnixNano(),
				StatusCode:     200,
				ExpectedStatus: 200,
				IsSuccessful:   true,
				ActualResponseBody: makeJSON(TargetResponse{
					OrderID:        "ORD-4",
					Type:           "LIMIT",
					Side:           "SELL",
					OrderedQty:     10,
					FilledQty:      10,
					Price:          100.00,
					ExecutionPrice: 99.00, // Worse than limit (Violation)
				}),
			},
			expected: false,
		},
		{
			name: "5. filled_qty > ordered_qty -> incorrect",
			event: events.OrderEvent{
				OrderID:        "ORD-5",
				SubmissionID:   "SUB-TEST",
				Type:           "LIMIT",
				Timestamp:      time.Now().UnixNano(),
				StatusCode:     200,
				ExpectedStatus: 200,
				IsSuccessful:   true,
				ActualResponseBody: makeJSON(TargetResponse{
					OrderID:        "ORD-5",
					Type:           "LIMIT",
					Side:           "BUY",
					OrderedQty:     10,
					FilledQty:      15, // Cannot create matter out of nowhere
					Price:          100.00,
					ExecutionPrice: 100.00,
				}),
			},
			expected: false,
		},
		{
			name: "6. Invalid JSON in response body -> incorrect",
			event: events.OrderEvent{
				OrderID:            "ORD-6",
				SubmissionID:       "SUB-TEST",
				Type:               "LIMIT",
				Timestamp:          time.Now().UnixNano(),
				StatusCode:         200,
				ExpectedStatus:     200,
				IsSuccessful:       true,
				ActualResponseBody: `{ "broken_json: true, }`,
			},
			expected: false,
		},
		{
			name: "7. MARKET order with any execution price -> correct",
			event: events.OrderEvent{
				OrderID:        "ORD-7",
				SubmissionID:   "SUB-TEST",
				Type:           "MARKET",
				Timestamp:      time.Now().UnixNano(),
				StatusCode:     200,
				ExpectedStatus: 200,
				IsSuccessful:   true,
				ActualResponseBody: makeJSON(TargetResponse{
					OrderID:        "ORD-7",
					Type:           "MARKET",
					Side:           "BUY",
					OrderedQty:     10,
					FilledQty:      10,
					Price:          0.00,
					ExecutionPrice: 9999.99, // Market orders take whatever is available
				}),
			},
			expected: true,
		},
		{
			name: "8. CANCEL order -> correct if status = cancelled",
			event: events.OrderEvent{
				OrderID:        "ORD-8",
				SubmissionID:   "SUB-TEST",
				Type:           "CANCEL",
				Timestamp:      time.Now().UnixNano(),
				StatusCode:     200,
				ExpectedStatus: 200,
				IsSuccessful:   true,
				ActualResponseBody: makeJSON(TargetResponse{
					OrderID: "ORD-8",
					Type:    "CANCEL",
					Status:  "cancelled", // Required confirmation string
				}),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize a fresh OrderBook for each test to guarantee no state bleeding
			ob := NewOrderBook()

			result := ob.ValidateOrder(tt.event)
			if result != tt.expected {
				t.Errorf("Test '%s' failed: expected %v, got %v", tt.name, tt.expected, result)
			}
		})
	}
}
