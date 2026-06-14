output "cluster_endpoint" {
  description = "The IP address of the GKE cluster control plane"
  value       = google_container_cluster.primary.endpoint
}

output "registry_url" {
  description = "The base URL for the Artifact Registry repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.repo.name}"
}

output "cluster_name" {
  description = "The name of the GKE cluster"
  value       = google_container_cluster.primary.name
}

output "region" {
  description = "The region of the GKE cluster"
  value       = var.region
}