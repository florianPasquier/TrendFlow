# create Dataset
resource "google_bigquery_dataset" "trendflow_dataset" {
  dataset_id    = "trendflow"
  friendly_name = "TrendFlow Dataset"
  description   = "Dataset for trend tracking"
  location      = var.region
}

# create Table
resource "google_bigquery_table" "ecommerce_sales_table" {
  dataset_id = google_bigquery_dataset.trendflow_dataset.dataset_id
  table_id   = "sales"

  schema = jsonencode([
    {
      name = "Date"
      type = "DATE"
      mode = "REQUIRED"
    },
    {
      name = "ASIN"
      type = "STRING"
      mode = "REQUIRED"
    },
      {
      name = "Product_Name"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "Qty"
      type = "INT64"
      mode = "NULLABLE"
    },
    {
      name = "Sales_Channel"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Category"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])
}

resource "google_storage_bucket" "trendflow-bucket" {
  name     = "${var.project_id}-trendflow-bucket" #(1)!
  location = var.region
}