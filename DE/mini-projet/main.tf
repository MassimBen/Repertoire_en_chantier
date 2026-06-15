# Création des ressource gcp

# création d'un bucket pour les données (CSV/parquet)

resource "google_storage_bucket" "data-lake" {
  name          = var.project_id
  location      = var.location
  force_destroy = true

  # AJOUTE CES DEUX LIGNES POUR RESPECTER LA SÉCURITÉ DE GCP
  public_access_prevention    = "enforced"
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}




# création d'un dataset biquery 

resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id = var.gcp_dataset
  location   = var.location
}

