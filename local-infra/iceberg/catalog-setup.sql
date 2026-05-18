-- PostgreSQL Schema setup for Apache Iceberg JDBC Catalog
-- This database schema tracks metadata table pointers, namespaces, and commits.

CREATE SCHEMA IF NOT EXISTS iceberg_catalog;

-- Iceberg Namespace Table: stores namespace hierarchy
CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_namespaces (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    PRIMARY KEY (catalog_name, namespace)
);

-- Iceberg Tables Table: stores metadata locations for tables
CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1024) NOT NULL,
    previous_metadata_location VARCHAR(1024),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

-- Seed initial namespace for Muninn
INSERT INTO iceberg_catalog.iceberg_namespaces (catalog_name, namespace) 
VALUES ('muninn_catalog', 'features')
ON CONFLICT DO NOTHING;
