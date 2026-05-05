# ------------------------------------------------------------------------------
# IAM Policies
# ------------------------------------------------------------------------------
resource "aws_iam_policy" "task_role_policy" {
  name = "pds-registry-sweeper-ecs-task-role-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "aoss:Get*",
          "aoss:List*",
          "aoss:Batch*",
          "aoss:APIAccessAll"
        ]
        Resource = "arn:aws:aoss:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:collection/${var.aoss_collection_id}"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
          "logs:CreateLogStream",
          "logs:CreateLogGroup"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "execution_role_policy" {
  name = "pds-registry-task-execution-role-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "ssm:GetParameters"
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = "logs:CreateLogGroup"
        Resource = "*"
      }
    ]
  })
}

# ------------------------------------------------------------------------------
# IAM Roles
# ------------------------------------------------------------------------------
resource "aws_iam_role" "task_role" {
  name                 = "pds-registry-sweeper-ecs-task-role"
  max_session_duration = 3600
  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.permissions_boundary_policy_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "ecs-tasks.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_role" "execution_role" {
  name                 = "pds-registry-sweeper-task-execution-role"
  max_session_duration = 3600
  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/${var.permissions_boundary_policy_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "ecs-tasks.amazonaws.com" }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })

  tags = local.tags

}

# Allow sweeper to write cloudwatch logs
resource "aws_iam_policy" "write_cloudwatch_logs" {
  name = "registry-sweeeper-cloudwatch-logs-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
            "logs:PutLogEvents",
            "logs:DescribeLogStreams",
            "logs:CreateLogStream",
            "logs:CreateLogGroup",
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_role_write_cloudwatch" {
  role       = aws_iam_role.task_role.name
  policy_arn = aws_iam_policy.write_cloudwatch_logs.arn
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_write_cloudwatch" {
  role       = aws_iam_role.execution_role.name
  policy_arn = aws_iam_policy.write_cloudwatch_logs.arn
}

# Allow the MWAA execution role to pass the ECS roles when calling RunTask
resource "aws_iam_policy" "mwaa_ecs_passrole_policy" {
  name = "pds-registry-sweeper-mwaa-ecs-passrole-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = [
          aws_iam_role.task_role.arn,
          aws_iam_role.execution_role.arn,
        ]
      }
    ]
  })
}



resource "aws_iam_role_policy_attachment" "mwaa_ecs_passrole" {
  role       = var.mwaa_execution_role_name
  policy_arn = aws_iam_policy.mwaa_ecs_passrole_policy.arn
}

resource "aws_iam_policy" "opensearch_api_only_access" {
  name        = "aoss-${var.aoss_collection_id}-limited-writer-access"
  description = "IAM policy for OpenSearch Serverless writer access, to be used by nodes through their Cognito user groups"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "aoss:APIAccessAll",
        ]
        Resource = "arn:aws:aoss:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:collection/${var.aoss_collection_id}"
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "opensearch_task_role_policy" {
  role       = aws_iam_role.task_role.name
  policy_arn = aws_iam_policy.opensearch_api_only_access.arn
}

# ------------------------------------------------------------------------------
# IAM Role Policy Attachments
# ------------------------------------------------------------------------------
resource "aws_iam_role_policy_attachment" "task_role_policy" {
  role       = aws_iam_role.task_role.name
  policy_arn = aws_iam_policy.task_role_policy.arn
}

resource "aws_iam_role_policy_attachment" "execution_role_policy" {
  role       = aws_iam_role.execution_role.name
  policy_arn = aws_iam_policy.execution_role_policy.arn
}

resource "aws_iam_role_policy_attachment" "execution_role_aws_managed" {
  role       = aws_iam_role.execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
