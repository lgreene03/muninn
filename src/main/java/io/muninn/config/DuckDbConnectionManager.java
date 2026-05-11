package io.muninn.config;

import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DuckDbConnectionManager {
    private final DriverManagerDataSource dataSource;

    public DuckDbConnectionManager(String path) {
        this.dataSource = new DriverManagerDataSource();
        this.dataSource.setDriverClassName("org.duckdb.DuckDBDriver");
        this.dataSource.setUrl("jdbc:duckdb:" + path);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
