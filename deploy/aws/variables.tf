variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "ap-south-1"
}

variable "name" {
  description = "Prefix for every resource name."
  type        = string
  default     = "simgo"
}

variable "image_tag" {
  description = "Worker image tag in the ECR repository (push it before scaling workers up)."
  type        = string
  default     = "latest"
}

variable "lambda_zip" {
  description = "Path to the API Lambda zip built by `make lambda` in cloud/."
  type        = string
  default     = "../../cloud/dist/simgo-api-lambda.zip"
}

variable "max_workers" {
  description = "Upper bound on concurrent worker tasks."
  type        = number
  default     = 20
}

variable "worker_cpu" {
  description = "Worker task CPU units (1024 = 1 vCPU). Seeds run in parallel across vCPUs."
  type        = number
  default     = 2048
}

variable "worker_memory" {
  description = "Worker task memory in MiB."
  type        = number
  default     = 4096
}

variable "shard_timeout_minutes" {
  description = "Time limit for one shard's go test run. The queue's visibility timeout is set above it."
  type        = number
  default     = 30
}

variable "retention_days" {
  description = "How long traces and results are kept."
  type        = number
  default     = 30
}
