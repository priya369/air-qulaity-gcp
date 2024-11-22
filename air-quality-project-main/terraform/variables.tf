variable "project" {
  description = "Project"
  default     = "valid-verbena-437709-h5"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west6"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "air_quality"
}

variable "weather-historical-data-bucket"{
  description = "My Storage Bucket for historical data"
  #Update the below to a unique bucket name
  default     = "valid-verbena-437709-h5-historical_data_bucket"
}

variable "weather-new-data-bucket" {
  description = "My Storage Bucket for new data"
  #Update the below to a unique bucket name
  default     = "valid-verbena-437709-h5-new_data_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
} 
