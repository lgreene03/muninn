package io.muninn.feature.microstructure;

import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.OrderDeltaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;
import java.util.Comparator;

/**
 * Orchestrates synchronization and gap-handling for the {@link OrderBookL3}.
 *
 * <p>Handles out-of-order deltas, detects missing sequences, and coordinates
 * full book snapshots to maintain deterministic state reconstruction.</p>
 */
public class OrderBookSyncEngine {

    private static final Logger log = LoggerFactory.getLogger(OrderBookSyncEngine.class);

    private final OrderBookL3 book;
    private final PriorityQueue<OrderDeltaEvent> buffer;

    private long expectedSequenceNumber = -1;
    private SyncState state = SyncState.AWAITING_SNAPSHOT;

    public enum SyncState {
        AWAITING_SNAPSHOT,
        SYNCHRONIZED,
        DESYNCED
    }

    public OrderBookSyncEngine() {
        this.book = new OrderBookL3();
        // Buffer for out-of-order sequence reconstruction
        this.buffer = new PriorityQueue<>(Comparator.comparingLong(OrderDeltaEvent::sequenceNumber));
    }

    /**
     * Applies a base snapshot to initialize or reset the book.
     */
    public void applySnapshot(OrderBookSnapshotEvent snapshot) {
        log.atInfo()
           .addKeyValue("instrument", snapshot.instrument().symbol())
           .addKeyValue("sequence", snapshot.sequenceNumber())
           .log("Applying L3 order book snapshot");

        // Real L3 implementation would construct individual nodes from snapshot.
        // For Phase 9 demo, we sync sequence and clear buffers.
        this.expectedSequenceNumber = snapshot.sequenceNumber() + 1;
        this.state = SyncState.SYNCHRONIZED;
        this.buffer.clear();
    }

    /**
     * Applies a delta. Buffers if out of order. Drops if stale.
     */
    public void applyDelta(OrderDeltaEvent delta) {
        if (state == SyncState.AWAITING_SNAPSHOT) {
            log.atDebug().log("Dropping delta while awaiting snapshot");
            return;
        }

        if (delta.sequenceNumber() < expectedSequenceNumber) {
            log.atDebug().log("Dropping stale delta");
            return;
        }

        if (delta.sequenceNumber() > expectedSequenceNumber) {
            // Out of order — buffer it
            buffer.add(delta);
            if (buffer.size() > 1000) {
                log.atWarn().log("Sequence buffer overflow. Desync detected.");
                state = SyncState.DESYNCED;
            }
            return;
        }

        // Apply exactly the expected sequence
        processDelta(delta);

        // Drain buffer of any subsequent contiguous sequences
        while (!buffer.isEmpty() && buffer.peek().sequenceNumber() == expectedSequenceNumber) {
            processDelta(buffer.poll());
        }
    }

    private void processDelta(OrderDeltaEvent delta) {
        book.applyDelta(delta);
        expectedSequenceNumber = delta.sequenceNumber() + 1;
    }

    public OrderBookL3 getBook() {
        return book;
    }

    public SyncState getState() {
        return state;
    }
}
