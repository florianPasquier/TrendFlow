# this file will include the configuration 
# for the Terraform state and for the GCP provider used

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0.0"
    }
  }
  
  required_version = ">= 1.3.0"
  backend "local" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}