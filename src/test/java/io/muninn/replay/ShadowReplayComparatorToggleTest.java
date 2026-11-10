package io.muninn.replay;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The comparator can be turned off, and is on by default so standalone muninn is
 * unchanged. It was added because the comparator once subscribed to every bare
 * {@code features.<name>.v<N>} topic, including obi-bridge's {@code features.obi.v1} in
 * norse-stack. Its patterns are now anchored to {@code muninn.features.} (ADR-0016, pinned
 * by {@link FeatureTopicNamespaceTest}), so the switch is no longer needed for that.
 */
class ShadowReplayComparatorToggleTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(ReplayDivergenceDetector.class, ReplayDivergenceDetector::new)
            .withUserConfiguration(ShadowReplayComparator.class);

    @Test
    void onByDefault() {
        runner.run(context -> assertThat(context).hasSingleBean(ShadowReplayComparator.class));
    }

    @Test
    void offWhenDisabled() {
        runner.withPropertyValues("muninn.replay.shadow-comparator.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(ShadowReplayComparator.class));
    }
}
