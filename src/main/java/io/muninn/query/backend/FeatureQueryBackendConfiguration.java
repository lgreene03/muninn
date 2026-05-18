package io.muninn.query.backend;

import io.muninn.storage.QueryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Wires exactly one {@link FeatureQueryBackend} bean based on
 * {@code muninn.query.backend}.
 *
 * <p>The default is {@code duckdb}, matching every {@code local-*} profile.
 * The {@code production-reference} profile (or any Helm values override) sets
 * {@code muninn.query.backend=trino} to engage the Trino path.</p>
 *
 * <p>Only one branch instantiates per app start, so the Trino JDBC connection
 * pool is never opened when DuckDB is active and vice versa.</p>
 */
@Configuration
@EnableConfigurationProperties(TrinoQueryConfig.class)
public class FeatureQueryBackendConfiguration {

    public static final String PROPERTY = "muninn.query.backend";

    @Bean
    @ConditionalOnProperty(name = PROPERTY, havingValue = "duckdb", matchIfMissing = true)
    public FeatureQueryBackend duckDbFeatureQueryBackend(
            QueryService queryService,
            @Value("${muninn.storage.buckets.warehouse:muninn-warehouse}") String warehouseBucket
    ) {
        return new DuckDbFeatureQueryBackend(queryService, "s3://" + warehouseBucket);
    }

    @Bean
    @ConditionalOnProperty(name = PROPERTY, havingValue = "trino")
    public DataSource trinoDataSource(TrinoQueryConfig config) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("io.trino.jdbc.TrinoDriver");
        ds.setUrl(config.jdbcUrl());
        ds.setUsername(config.user());
        if (config.password() != null && !config.password().isBlank()) {
            ds.setPassword(config.password());
        }
        // Trino prefers connection properties over URL parameters for some flags;
        // keeping the properties bag empty here is fine for the read-only path.
        ds.setConnectionProperties(new Properties());
        return ds;
    }

    @Bean
    @ConditionalOnProperty(name = PROPERTY, havingValue = "trino")
    public FeatureQueryBackend trinoFeatureQueryBackend(
            DataSource trinoDataSource,
            TrinoQueryConfig config
    ) {
        return new TrinoFeatureQueryBackend(trinoDataSource, config.catalog(), config.schema());
    }
}
