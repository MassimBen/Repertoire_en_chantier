variable "project_id" {
  description = "ID projet"
  type        = string
  default     = "project-03262434-f852-4c90-b66"
}

variable "region" {
  description = "Region default des ressource GCP"
  type        = string
  default     = "europe-west1"
}

variable "location" {
  description = "La localisation géographique pour le Bucket et BigQuery"
  type        = string
  default     = "EU"
}

variable "gcp_dataset" {
  description = " Dataset in GCP"
  type        = string
  default     = "trip_all"
}



