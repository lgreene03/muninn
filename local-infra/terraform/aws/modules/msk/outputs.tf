output "bootstrap_brokers" {
  value       = aws_msk_cluster.main.bootstrap_brokers
  description = "Plaintext connection string for MSK brokers"
}

output "bootstrap_brokers_tls" {
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
  description = "Secure TLS connection string for MSK brokers"
}
