resource "aws_security_group" "msk" {
  name        = "${var.project}-${var.environment}-sg-msk"
  description = "Access to MSK brokers"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Bounded in actual production VPC configurations
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-${var.environment}-sg-msk"
  }
}

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project}-${var.environment}-kafka"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = var.broker_nodes

  broker_node_group_info {
    instance_type = var.instance_type
    client_subnets = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]
    
    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  tags = {
    Name = "${var.project}-${var.environment}-kafka"
  }
}
