variable "project_id" {
    type        = string
    default     = "trendflow-455409"
    description = "GCP project id where the resources will be created into."
}

variable "region" {
    type        = string
    default     = "europe-west1"
    description = "Location where the GCP resources will be created"
}