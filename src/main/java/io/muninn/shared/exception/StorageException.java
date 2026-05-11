package io.muninn.shared.exception;

/**
 * Thrown when a storage operation fails — DuckDB queries, Parquet writes, MinIO operations.
 */
public class StorageException extends MuninnException {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
