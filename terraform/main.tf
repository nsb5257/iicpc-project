terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "apis" {
  for_each = toset([
    "container.googleapis.com",
    "artifactregistry.googleapis.com"
  ])
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "repo" {
  depends_on    = [google_project_service.apis]
  location      = var.region
  repository_id = "iicpc-repo"
  description   = "Docker repository for IICPC Platform images"
  format        = "DOCKER"
}

# Provision Standard GKE Cluster
resource "google_container_cluster" "primary" {
  depends_on = [google_project_service.apis]
  name       = var.cluster_name
  location   = var.region

  # Remove default node pool to define a custom one
  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false
}

# Custom Node Pool for Standard GKE
resource "google_container_node_pool" "primary_nodes" {
  name       = "primary-node-pool"
  location   = var.region
  cluster    = google_container_cluster.primary.name
  
  # 1 node per zone (us-central1 has 3 zones, so 3 nodes total)
  node_count = 1 

  node_config {
    machine_type = "e2-standard-4"
    disk_size_gb = 50
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}