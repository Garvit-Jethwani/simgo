output "api_url" {
  description = "Base URL of the SimGo API."
  value       = aws_lambda_function_url.api.function_url
}

output "api_token" {
  description = "Bearer token for the API (terraform output -raw api_token)."
  value       = random_password.api_token.result
  sensitive   = true
}

output "worker_repository_url" {
  description = "Push the worker image here."
  value       = aws_ecr_repository.worker.repository_url
}

output "queue_url" {
  value = aws_sqs_queue.shards.url
}

output "dead_letter_queue_url" {
  value = aws_sqs_queue.dlq.url
}

output "bucket" {
  value = aws_s3_bucket.traces.bucket
}

output "table" {
  value = aws_dynamodb_table.results.name
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}
