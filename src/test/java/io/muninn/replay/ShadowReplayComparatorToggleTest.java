package io.muninn.replay;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * The comparator subscribes to every {@code features.<name>.v<N>} topic, so a deployment
 * where another system writes one (norse-stack's obi-bridge writes {@code features.obi.v1})
 * must be able to turn it off. On by default, so standalone muninn is unchanged.
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
