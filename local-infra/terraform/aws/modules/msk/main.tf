# MSK security group.
#
# Ingress is bounded to the VPC CIDR (passed as a variable) — not 0.0.0.0/0.
# Brokers are private; clients reach them from inside the VPC. The previous
# 0.0.0.0/0 rule was tagged as "tightened in production"; that tightening is
# now the default, with an opt-in escape hatch via additional_ingress_cidrs.

data "aws_vpc" "this" {
  id = var.vpc_id
}

resource "aws_security_group" "msk" {
  name        = "${var.project}-${var.environment}-sg-msk"
  description = "Access to MSK brokers — VPC-bounded; clients must be in-VPC"
  vpc_id      = var.vpc_id

  # Plaintext + TLS + IAM-auth ports. Plaintext is still accepted only because
  # MSK requires the security group to permit all of them when the cluster
  # advertises multiple protocols; the cluster itself only accepts TLS below.
  ingress {
    from_port   = 9092
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = concat([data.aws_vpc.this.cidr_block], var.additional_ingress_cidrs)
    description = "Kafka client access from within the VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Outbound to AWS APIs and broker peers"
  }

  tags = {
    Name = "${var.project}-${var.environment}-sg-msk"
  }
}

# CloudWatch log group for broker logs. Required so MSK has a non-S3 sink for
# durable broker-side logging; the retention is bounded to keep cost predictable.
resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.project}-${var.environment}"
  retention_in_days = var.log_retention_days
}

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project}-${var.environment}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_nodes

  broker_node_group_info {
    instance_type   = var.instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_size_gb
      }
    }
  }

  # TLS required for client-broker. At-rest encryption uses the AWS-managed
  # KMS key for MSK by default; pin a CMK via var.kms_key_arn if compliance
  # requires it.
  encryption_info {
    encryption_at_rest_kms_key_arn = var.kms_key_arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  # Broker logs to CloudWatch so operators can audit auth failures without
  # pulling a private subnet shell.
  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  # Enhanced monitoring surfaces topic/partition/consumer-group metrics to
  # CloudWatch — required to wire the metrics named in OBSERVABILITY_STRATEGY.md.
  enhanced_monitoring = var.enhanced_monitoring

  tags = {
    Name = "${var.project}-${var.environment}-kafka"
  }
}
