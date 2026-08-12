package com.jstream.service;

import com.jstream.models.BrokerMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class TopicService {
    private final Map<String, Sinks.Many<BrokerMessage>> topicSinks = new ConcurrentHashMap<>();

    private Sinks.Many<BrokerMessage> getOrCreateSink(String topic) {
        return topicSinks.computeIfAbsent(topic, key -> {
            log.info("[TopicService] Creating new sink for topic {}", topic);
            return Sinks.many().multicast().onBackpressureBuffer();
        });
    }

    public void publish(String topic, BrokerMessage message) {
        Sinks.Many<BrokerMessage> sink = getOrCreateSink(topic);
        sink.tryEmitNext(message);
    }

    public Flux<BrokerMessage> subscribe(String topic) {
        Sinks.Many<BrokerMessage> sink = getOrCreateSink(topic);
        return sink.asFlux();
    }
}
