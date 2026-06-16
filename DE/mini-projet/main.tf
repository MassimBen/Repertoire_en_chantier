# Création des ressource gcp

# création d'un bucket pour les données (CSV/parquet)

resource "google_storage_bucket" "data-lake" {
  name          = var.project_id
  location      = var.location
  force_destroy = true

  #  Ajout de ces deux lignes pour respecter la sécurité de GCP 
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

