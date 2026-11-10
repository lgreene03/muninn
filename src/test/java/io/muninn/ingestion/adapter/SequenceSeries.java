package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

/** For the live smoke tests: what each recorded series must look like on real traffic. */
final class SequenceSeries {

    private SequenceSeries() {
    }

    /**
     * Every (instrument, event type) series in arrival order counts 1, 2, 3 without a gap
     * or a repeat: no message was attributed and then lost, and none arrived twice.
     */
    static void assertEachCountsFromOneWithoutGaps(List<MarketEvent> events) {
        Map<String, List<Long>> series = new LinkedHashMap<>();
        for (MarketEvent e : events) {
            String key = e.instrument().symbol() + " " + (e instanceof TradeEvent ? "trades" : "books");
            series.computeIfAbsent(key, k -> new ArrayList<>()).add(e.sequenceNumber());
        }
        assertThat(series).isNotEmpty();
        series.forEach((key, numbers) -> assertThat(numbers).as(key)
                .isEqualTo(LongStream.rangeClosed(1, numbers.size()).boxed().toList()));
    }
}
