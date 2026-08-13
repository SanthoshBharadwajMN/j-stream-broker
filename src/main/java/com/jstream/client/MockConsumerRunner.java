package com.jstream.client;

import com.jstream.models.BrokerMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Slf4j
@Component
public class MockConsumerRunner implements CommandLineRunner {
    private final RSocketRequester rSocketRequester;

    public MockConsumerRunner(RSocketRequester.Builder builder) {
        rSocketRequester = builder.tcp("localhost", 7000);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("[MockConsumer] Starting mock consumer. Connecting to MessageBroker via TCP");

        Sinks.Many<Long> ackSink = Sinks.many().unicast().onBackpressureBuffer();

        Flux<BrokerMessage> incomingStream = rSocketRequester
                .route("subscribe.test1.testconsumer")
                .data(ackSink.asFlux(), Long.class)
                .retrieveFlux(BrokerMessage.class);

        // dummy emit. Without this emit, the ".data()" above was not functioning.
        ackSink.tryEmitNext(-1L);

        incomingStream.subscribe(
                message -> {
                    log.info("[MockConsumer] MockConsumerRunner received: {}", message);
                    ackSink.tryEmitNext(message.offset());
                },
                error -> log.error("Consumer stream crashed", error)
        );
    }
}
