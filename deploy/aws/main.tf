data "aws_caller_identity" "current" {}

locals {
  visibility_timeout = (var.shard_timeout_minutes + 5) * 60
}

# ------------------------------------------------------------------ storage

resource "aws_s3_bucket" "traces" {
  bucket_prefix = "${var.name}-traces-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "traces" {
  bucket                  = aws_s3_bucket.traces.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "traces" {
  bucket = aws_s3_bucket.traces.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "traces" {
  bucket = aws_s3_bucket.traces.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "traces" {
  bucket = aws_s3_bucket.traces.id
  rule {
    id     = "expire-traces"
    status = "Enabled"
    filter {}
    expiration {
      days = var.retention_days
    }
  }
}

resource "aws_dynamodb_table" "results" {
  name         = "${var.name}-results"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
  ttl {
    attribute_name = "expires"
    enabled        = true
  }
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-shards-dlq"
  message_retention_seconds = 1209600 # 14 days
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "shards" {
  name                       = "${var.name}-shards"
  visibility_timeout_seconds = local.visibility_timeout
  message_retention_seconds  = 345600 # 4 days
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_ecr_repository" "worker" {
  name                 = "${var.name}-worker"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "worker" {
  repository = aws_ecr_repository.worker.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep the last 10 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10 }
      action       = { type = "expire" }
    }]
  })
}

resource "random_password" "api_token" {
  length  = 40
  special = false
}

# --------------------------------------------------------------------- IAM

data "aws_iam_policy_document" "data_access" {
  statement {
    sid       = "Queue"
    actions   = ["sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.shards.arn]
  }
  statement {
    sid       = "Results"
    actions   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:Query"]
    resources = [aws_dynamodb_table.results.arn]
  }
  statement {
    sid       = "Traces"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.traces.arn}/traces/*"]
  }
}

resource "aws_iam_policy" "data_access" {
  name   = "${var.name}-data-access"
  policy = data.aws_iam_policy_document.data_access.json
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "worker_execution" {
  name               = "${var.name}-worker-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "worker_execution" {
  role       = aws_iam_role.worker_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "worker_task" {
  name               = "${var.name}-worker-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "worker_task" {
  role       = aws_iam_role.worker_task.name
  policy_arn = aws_iam_policy.data_access.arn
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "api" {
  name               = "${var.name}-api"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "api_logs" {
  role       = aws_iam_role.api.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "api_data" {
  role       = aws_iam_role.api.name
  policy_arn = aws_iam_policy.data_access.arn
}

# ----------------------------------------------------------------- workers

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/${var.name}/worker"
  retention_in_days = 14
}

resource "aws_ecs_cluster" "this" {
  name = var.name
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]
  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }
}

resource "aws_ecs_task_definition" "worker" {
  family                   = "${var.name}-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.worker_execution.arn
  task_role_arn            = aws_iam_role.worker_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  ephemeral_storage {
    size_in_gib = 40 # checkouts, module cache, build cache
  }

  container_definitions = jsonencode([{
    name      = "worker"
    image     = "${aws_ecr_repository.worker.repository_url}:${var.image_tag}"
    essential = true
    command   = ["worker", "-shard-timeout", "${var.shard_timeout_minutes}m"]
    environment = [
      { name = "SIMGO_QUEUE_URL", value = aws_sqs_queue.shards.url },
      { name = "SIMGO_BUCKET", value = aws_s3_bucket.traces.bucket },
      { name = "SIMGO_TABLE", value = aws_dynamodb_table.results.name },
      { name = "AWS_REGION", value = var.region },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.worker.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "worker"
      }
    }
  }])
}

# Workers need outbound internet access (git, Go modules, AWS APIs) and no
# inbound access. The default VPC's public subnets avoid a NAT gateway.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "worker" {
  name        = "${var.name}-worker"
  description = "SimGo workers: outbound only"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "outbound for git, Go modules and AWS APIs"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecs_service" "worker" {
  name            = "${var.name}-worker"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = 0 # scaled by queue depth

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.worker.id]
    assign_public_ip = true
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}

# Scale on queue depth: up when shards are waiting, down to zero when the
# queue (waiting + in flight) has been empty for 15 minutes.
resource "aws_appautoscaling_target" "worker" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.worker.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = 0
  max_capacity       = var.max_workers
}

resource "aws_appautoscaling_policy" "scale_out" {
  name               = "${var.name}-scale-out"
  service_namespace  = aws_appautoscaling_target.worker.service_namespace
  resource_id        = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = aws_appautoscaling_target.worker.scalable_dimension
  policy_type        = "StepScaling"

  step_scaling_policy_configuration {
    adjustment_type         = "ExactCapacity"
    cooldown                = 60
    metric_aggregation_type = "Maximum"

    step_adjustment {
      metric_interval_lower_bound = 0
      metric_interval_upper_bound = 10
      scaling_adjustment          = min(2, var.max_workers)
    }
    step_adjustment {
      metric_interval_lower_bound = 10
      metric_interval_upper_bound = 100
      scaling_adjustment          = min(8, var.max_workers)
    }
    step_adjustment {
      metric_interval_lower_bound = 100
      scaling_adjustment          = var.max_workers
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "backlog" {
  alarm_name          = "${var.name}-shards-waiting"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.shards.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  alarm_actions       = [aws_appautoscaling_policy.scale_out.arn]
}

resource "aws_appautoscaling_policy" "scale_in" {
  name               = "${var.name}-scale-in"
  service_namespace  = aws_appautoscaling_target.worker.service_namespace
  resource_id        = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = aws_appautoscaling_target.worker.scalable_dimension
  policy_type        = "StepScaling"

  step_scaling_policy_configuration {
    adjustment_type = "ExactCapacity"
    cooldown        = 300
    step_adjustment {
      metric_interval_upper_bound = 0
      scaling_adjustment          = 0
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "idle" {
  alarm_name          = "${var.name}-queue-idle"
  comparison_operator = "LessThanOrEqualToThreshold"
  threshold           = 0
  evaluation_periods  = 15
  datapoints_to_alarm = 15
  alarm_actions       = [aws_appautoscaling_policy.scale_in.arn]

  metric_query {
    id          = "total"
    expression  = "visible + inflight"
    label       = "shards waiting or running"
    return_data = true
  }
  metric_query {
    id = "visible"
    metric {
      namespace   = "AWS/SQS"
      metric_name = "ApproximateNumberOfMessagesVisible"
      dimensions  = { QueueName = aws_sqs_queue.shards.name }
      stat        = "Maximum"
      period      = 60
    }
  }
  metric_query {
    id = "inflight"
    metric {
      namespace   = "AWS/SQS"
      metric_name = "ApproximateNumberOfMessagesNotVisible"
      dimensions  = { QueueName = aws_sqs_queue.shards.name }
      stat        = "Maximum"
      period      = 60
    }
  }
}

# --------------------------------------------------------------------- API

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${var.name}-api"
  retention_in_days = 14
}

resource "aws_lambda_function" "api" {
  function_name    = "${var.name}-api"
  role             = aws_iam_role.api.arn
  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  memory_size      = 256
  timeout          = 30

  environment {
    variables = {
      SIMGO_QUEUE_URL = aws_sqs_queue.shards.url
      SIMGO_BUCKET    = aws_s3_bucket.traces.bucket
      SIMGO_TABLE     = aws_dynamodb_table.results.name
      SIMGO_API_TOKEN = random_password.api_token.result
    }
  }

  depends_on = [aws_cloudwatch_log_group.api]
}

# The function URL is public; every endpoint except /healthz checks the
# bearer token.
resource "aws_lambda_function_url" "api" {
  function_name      = aws_lambda_function.api.function_name
  authorization_type = "NONE"
}
