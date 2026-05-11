package io.muninn.storage;

import io.muninn.event.EventEnvelope;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface ParquetWriter {

    Path write(String dataset, List<EventEnvelope> envelopes) throws IOException;
}
