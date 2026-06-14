#!/bin/bash
set -e

# Hardcoded to your exact GCP Project ID
REGISTRY=${REGISTRY:-"us-central1-docker.pkg.dev/active-freehold-499017-q0/iicpc-repo"}
TAG=${TAG:-"latest"}
IMAGE="${REGISTRY}/iicpc-platform:${TAG}"

echo "Building unified monolithic image for linux/amd64: $IMAGE"
docker build --platform linux/amd64 -t "$IMAGE" .

echo "Pushing image to GCP Artifact Registry..."
docker push "$IMAGE"

echo "✅ Successfully built and pushed $IMAGE"