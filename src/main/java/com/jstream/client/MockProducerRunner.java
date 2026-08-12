package com.jstream.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
@Component
public class MockProducerRunner implements CommandLineRunner {
    private final RSocketRequester rSocketRequester;

    public MockProducerRunner(RSocketRequester.Builder builder) {
        rSocketRequester = builder.tcp("localhost", 7000);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting mock producer. Connecting to broker via TCP");
        Flux.interval(Duration.ofSeconds(2))
                .flatMap(i -> {
                    String message = "Hi, message ID " + i;
                    return rSocketRequester
                            .route("publish.test1")
                            .data(message)
                            .send();
                })
                .subscribe();
    }
}
