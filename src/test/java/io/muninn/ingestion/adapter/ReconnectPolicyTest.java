package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ReconnectPolicyTest {

    private static ReconnectPolicy policy(boolean jitter, double random) {
        return new ReconnectPolicy(new BinanceConfig.Reconnect(1000, 30_000, jitter), () -> random);
    }

    private static long reconnect(ReconnectPolicy p) {
        long delay = p.onDisconnect();
        p.onReconnectStarted();
        return delay;
    }

    @Test
    void backoffEscalatesAcrossReconnects() {
        // The Binance adapter's defect: its count lived on a per-connection listener, so
        // every reconnect was attempt one. Here it persists across connections.
        ReconnectPolicy p = policy(false, 0);
        assertThat(new long[] {reconnect(p), reconnect(p), reconnect(p), reconnect(p)})
                .containsExactly(1000, 2000, 4000, 8000);
    }

    @Test
    void backoffIsCapped() {
        ReconnectPolicy p = policy(false, 0);
        long last = 0;
        for (int i = 0; i < 40; i++) {
            last = reconnect(p);
        }
        assertThat(last).isEqualTo(30_000);
    }

    @Test
    void aCloseAndAnErrorForTheSameFailureScheduleOneReconnect() {
        // onClose and onError can both fire for one broken socket. Two reconnects means
        // two sockets and every event duplicated.
        ReconnectPolicy p = policy(false, 0);
        assertThat(p.onDisconnect()).isEqualTo(1000);
        assertThat(p.onDisconnect()).isEqualTo(ReconnectPolicy.ALREADY_PENDING);
        assertThat(p.onDisconnect()).isEqualTo(ReconnectPolicy.ALREADY_PENDING);
        assertThat(p.consecutiveFailures()).as("counted once").isEqualTo(1);

        p.onReconnectStarted();
        assertThat(p.onDisconnect()).as("the next real failure schedules again").isEqualTo(2000);
    }

    @Test
    void onlyRealDataResetsTheBackoff() {
        // A server that accepts and then immediately closes must keep backing off;
        // merely reconnecting is not evidence the connection works.
        ReconnectPolicy p = policy(false, 0);
        reconnect(p);
        reconnect(p);
        assertThat(reconnect(p)).isEqualTo(4000);
        p.onHealthy();
        assertThat(reconnect(p)).isEqualTo(1000);
    }

    @Test
    void jitterStaysWithinTheUpperHalfOfTheDelay() {
        assertThat(policy(true, 0.0).delayFor(3)).isEqualTo(2000); // half of 4000
        assertThat(policy(true, 0.999).delayFor(3)).isBetween(3990L, 4000L);
    }
}
