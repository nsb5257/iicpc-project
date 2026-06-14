#!/bin/bash
set -e

# Exporting this prevents Terraform from prompting you for the project ID
export TF_VAR_project_id="active-freehold-499017-q0"

echo "Applying Terraform infrastructure (Auto-Approve)..."
cd terraform
terraform init
terraform apply -auto-approve

# Extract exact values from terraform state
CLUSTER_NAME=$(terraform output -raw cluster_name 2>/dev/null || echo "iicpc-cluster")
REGION=$(terraform output -raw region 2>/dev/null || echo "us-central1")
cd ..

echo "Fetching GKE cluster credentials..."
gcloud container clusters get-credentials "$CLUSTER_NAME" --region "$REGION" --project "$TF_VAR_project_id"

echo "Applying Kubernetes manifests in strict dependency order..."

# 1. Credentials and Config
kubectl apply -f k8s/secrets.yaml

# 2. Stateful Infrastructure
kubectl apply -f k8s/timescaledb.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/redpanda.yaml

echo "Waiting 15 seconds for stateful infrastructure to initialize..."
sleep 15

# 3. Application Services
kubectl apply -f k8s/sandbox.yaml
kubectl apply -f k8s/telemetry.yaml
kubectl apply -f k8s/leaderboard.yaml
kubectl apply -f k8s/fleet.yaml

# 4. Autoscaling Configurations
kubectl apply -f k8s/hpa-fleet.yaml
kubectl apply -f k8s/hpa-telemetry.yaml

echo "Waiting for Application Rollouts to complete..."
kubectl rollout status deployment/sandbox-deployment
kubectl rollout status deployment/telemetry-deployment
kubectl rollout status deployment/leaderboard-deployment
kubectl rollout status deployment/fleet-deployment

echo "✅ Full Platform Deployment Complete!"