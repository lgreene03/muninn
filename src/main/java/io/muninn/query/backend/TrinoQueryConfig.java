package io.muninn.query.backend;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Trino connection configuration. Inactive unless {@code muninn.query.backend=trino}.
 *
 * <p>Defaults point at a local docker-compose Trino on {@code localhost:8080}
 * with the {@code iceberg} catalog and {@code muninn} schema — placeholders
 * that an operator overrides in the {@code production-reference} Helm values.</p>
 *
 * <p>Credentials are read from the environment in the production-reference
 * Helm chart so they're never committed.</p>
 *
 * @param host     Trino coordinator host
 * @param port     Trino coordinator port (HTTP, not HTTPS — the LB usually terminates TLS)
 * @param user     Trino user; Glue IAM handles authorization downstream
 * @param password optional password (null for IAM/JWT-fronted clusters)
 * @param catalog  Iceberg catalog name as registered in Trino
 * @param schema   Trino schema name (one schema per environment is conventional)
 * @param ssl      whether to wrap the JDBC connection in TLS
 */
@ConfigurationProperties(prefix = "muninn.query.trino")
public record TrinoQueryConfig(
        String host,
        int port,
        String user,
        String password,
        String catalog,
        String schema,
        boolean ssl
) {

    public TrinoQueryConfig {
        if (host == null || host.isBlank()) host = "localhost";
        if (port <= 0) port = 8080;
        if (user == null || user.isBlank()) user = "muninn";
        if (catalog == null || catalog.isBlank()) catalog = "iceberg";
        if (schema == null || schema.isBlank()) schema = "muninn";
    }

    /**
     * Build the JDBC URL.
     *
     * <p>The driver accepts SSL as a query parameter rather than a separate URL
     * scheme; we render it consistently here so the data-source bean stays
     * configuration-free.</p>
     */
    public String jdbcUrl() {
        String url = "jdbc:trino://" + host + ":" + port + "/" + catalog + "/" + schema;
        return ssl ? url + "?SSL=true" : url;
    }
}
