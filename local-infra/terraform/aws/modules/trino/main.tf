resource "aws_security_group" "trino" {
  name        = "${var.project}-${var.environment}-sg-trino"
  description = "Access to Trino coordinator inside VPC"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-${var.environment}-sg-trino"
  }
}

resource "aws_iam_policy" "trino_s3_glue_access" {
  name        = "${var.project}-${var.environment}-trino-s3-glue-policy"
  description = "Policy for Trino accessing S3 warehouse and Glue catalog metadata"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = ["*"]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable"
        ]
        Resource = ["*"]
      }
    ]
  })
}
