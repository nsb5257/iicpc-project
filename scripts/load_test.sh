#!/bin/bash
set -e

echo "Polling Kubernetes for External LoadBalancer IPs..."
while true; do
    SANDBOX_IP=$(kubectl get svc sandbox-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
    FLEET_IP=$(kubectl get svc fleet-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
    LEADERBOARD_IP=$(kubectl get svc leaderboard-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
    
    if [[ -n "$SANDBOX_IP" && -n "$FLEET_IP" ]]; then
        break
    fi
    echo "Waiting for GCP to provision external IPs..."
    sleep 5
done

echo "✅ Sandbox IP: $SANDBOX_IP"
echo "✅ Fleet IP: $FLEET_IP"
echo "----------------------------------------------------"

# Function to upload a bot, parse the endpoint, and trigger the fleet
upload_and_run() {
    local lang=$1
    local file=$2
    local sub_id="sub-${lang}-$(date +%s)"

    echo "Uploading $lang bot ($sub_id)..."
    UPLOAD_RESP=$(curl -s -X POST -F "submission_id=$sub_id" -F "language=$lang" -F "source_file=@$file" "http://$SANDBOX_IP:8081/upload")
    
    # Extract IP and Port without needing jq dependency
    CONTAINER_IP=$(echo "$UPLOAD_RESP" | grep -o '"container_ip":"[^"]*' | cut -d'"' -f4)
    CONTAINER_PORT=$(echo "$UPLOAD_RESP" | grep -o '"container_port":[0-9]*' | cut -d':' -f2)
    
    if [[ -z "$CONTAINER_IP" || -z "$CONTAINER_PORT" ]]; then
        echo "❌ Failed to parse container endpoint for $lang. Response: $UPLOAD_RESP"
        return
    fi
    
    ENDPOINT="${CONTAINER_IP}:${CONTAINER_PORT}"
    echo "✅ $lang Sandbox Container Ready at: $ENDPOINT"
    
    echo "Sleeping 3 seconds for container startup stabilization..."
    sleep 3

    echo "Triggering Fleet load test for $lang (200 Bots, 10k Orders, 500 TPS)..."
    curl -s -X POST -H "Content-Type: application/json" \
        -d "{\"submission_id\":\"$sub_id\", \"endpoint\":\"$ENDPOINT\", \"num_bots\":200, \"num_orders\":10000, \"tps\":500}" \
        "http://$FLEET_IP:8080/run"
    
    echo -e "\n✅ Load test fired for $sub_id\n"
}

upload_and_run "go" "test-bots/contestant.go"
upload_and_run "rust" "test-bots/contestant.rs"
upload_and_run "cpp" "test-bots/contestant.cpp"

echo "⏳ Waiting 90 seconds for load tests to complete and Redpanda -> TimescaleDB pipeline to flush..."
sleep 90

echo "Establishing secure port-forward to TimescaleDB..."
kubectl port-forward svc/timescaledb-service 5432:5432 > /dev/null 2>&1 &
PF_PID=$!
sleep 4 # Give port-forward time to bind

export PGPASSWORD="supersecretpassword"
QUERY="SELECT submission_id, COUNT(*) as orders, \
ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) as p50_ms, \
ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) as p90_ms, \
ROUND(percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)::numeric,2) as p99_ms, \
ROUND(100.0 * SUM(CASE WHEN is_correct THEN 1 ELSE 0 END)/COUNT(*),2) as correctness_pct \
FROM order_metrics WHERE is_successful = TRUE GROUP BY submission_id ORDER BY p90_ms;"

echo ""
echo "==================== 🏆 IICPC LOAD TEST RESULTS 🏆 ===================="
psql -h 127.0.0.1 -p 5432 -U iicpc_admin -d metrics_db -c "$QUERY"
echo "======================================================================="

kill $PF_PID

if [[ -n "$LEADERBOARD_IP" ]]; then
    echo "📊 Live Leaderboard UI is available at: http://$LEADERBOARD_IP:8085"
fi

# At the very end of the script
kill $PF_PID

if [[ -n "$LEADERBOARD_IP" ]]; then
    echo "📊 Live Leaderboard UI is available at: http://$LEADERBOARD_IP" # FIXED: Removed :8085
fi