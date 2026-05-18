package io.muninn.config;

import org.springframework.jdbc.datasource.DriverManagerDataSource;
import io.muninn.config.StorageConfig.S3Properties;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;

public class DuckDbConnectionManager {
    private final DriverManagerDataSource dataSource;
    private final S3Properties s3Properties;

    public DuckDbConnectionManager(String path, S3Properties s3Properties) {
        this.dataSource = new DriverManagerDataSource();
        this.dataSource.setDriverClassName("org.duckdb.DuckDBDriver");
        this.dataSource.setUrl("jdbc:duckdb:" + path);
        this.s3Properties = s3Properties;
    }

    public Connection getConnection() throws SQLException {
        Connection conn = dataSource.getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL httpfs;");
            stmt.execute("LOAD httpfs;");
            
            if (s3Properties != null) {
                String endpoint = s3Properties.endpoint().replace("http://", "").replace("https://", "");
                stmt.execute("SET s3_endpoint='" + endpoint + "';");
                stmt.execute("SET s3_access_key_id='" + s3Properties.accessKey() + "';");
                stmt.execute("SET s3_secret_access_key='" + s3Properties.secretKey() + "';");
                stmt.execute("SET s3_region='" + s3Properties.region() + "';");
                
                if (s3Properties.endpoint().startsWith("http://")) {
                    stmt.execute("SET s3_use_ssl=false;");
                }
                stmt.execute("SET s3_url_style='path';");
            }
        }
        return conn;
    }
}
