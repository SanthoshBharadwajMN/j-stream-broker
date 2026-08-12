package com.jstream.client;

import com.jstream.models.BrokerMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Slf4j
@Component
public class MockConsumerRunner implements CommandLineRunner {
    private final RSocketRequester rSocketRequester;

    public MockConsumerRunner(RSocketRequester.Builder builder) {
        rSocketRequester = builder.tcp("localhost", 7000);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting mock consumer. Connecting to MessageBroker via TCP");
        Flux<BrokerMessage> incomingStream = rSocketRequester
                .route("subscribe.test1.testconsumer")
                .retrieveFlux(BrokerMessage.class);

        incomingStream.subscribe(message -> {
            log.info("MockConsumerRunner received: {}", message);
        });
    }
}
