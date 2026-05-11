package io.muninn.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

public interface ObjectStore {

    void put(String bucket, String key, Path file) throws IOException;

    InputStream get(String bucket, String key) throws IOException;

    List<String> list(String bucket, String prefix) throws IOException;

    void delete(String bucket, String key) throws IOException;
}
