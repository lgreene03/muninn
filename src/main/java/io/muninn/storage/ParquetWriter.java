package io.muninn.storage;

import io.muninn.shared.event.MarketEvent;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Contract for writing market events to Parquet files.
 *
 * <p>Implementation deferred to Phase 3. The interface exists to define
 * the boundary between the stream processor and the storage layer.</p>
 */
public interface ParquetWriter {

    Path write(String dataset, List<MarketEvent> events) throws IOException;
}
