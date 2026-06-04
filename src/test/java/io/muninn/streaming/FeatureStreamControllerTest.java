package io.muninn.streaming;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FeatureStreamControllerTest {

    @Mock
    private FeatureStreamBroker broker;

    @InjectMocks
    private FeatureStreamController controller;

    @Test
    void stream_delegatesToBrokerWithFilter() {
        SseEmitter emitter = new SseEmitter();
        when(broker.register("vwap.1m")).thenReturn(emitter);

        SseEmitter result = controller.stream("vwap.1m");

        assertThat(result).isSameAs(emitter);
        verify(broker).register("vwap.1m");
    }

    @Test
    void stream_nullFilter_subscribesToAllFeatures() {
        SseEmitter emitter = new SseEmitter();
        when(broker.register(null)).thenReturn(emitter);

        SseEmitter result = controller.stream(null);

        assertThat(result).isSameAs(emitter);
        verify(broker).register(null);
    }
}
