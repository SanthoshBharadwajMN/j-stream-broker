package com.jstream.controller;

import com.jstream.models.BrokerMessage;
import com.jstream.repository.ConsumerOffsetRepository;
import com.jstream.service.TopicService;
import com.jstream.service.StorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Controller
public class MessageBrokerController {
    private final TopicService topicService;
    private final StorageService storageService;
    private final ConsumerOffsetRepository consumerOffsetRepository;

    @Autowired
    public MessageBrokerController(TopicService topicService, StorageService storageService, ConsumerOffsetRepository consumerOffsetRepository) {
        this.topicService = topicService;
        this.storageService = storageService;
        this.consumerOffsetRepository = consumerOffsetRepository;
    }

    @MessageMapping("publish.{topic}")
    public Mono<Void> publishMessage(@DestinationVariable("topic") String topic, String payload) {
        log.info("Received message from producer: {}", payload);
        long offset = storageService.append(topic, payload);
        BrokerMessage message = new BrokerMessage(offset, payload);
        log.debug("Saved message '{}' to disk at offset {}", payload, offset);
        topicService.publish(topic, message);
        return Mono.empty();
    }

    @MessageMapping("subscribe.{topic}.{group}")
    public Flux<BrokerMessage> subscribeToTopic(@DestinationVariable("topic") String topic, @DestinationVariable("group") String group) {
        return topicService.subscribe(topic)
                .flatMap(message -> {
                    return consumerOffsetRepository.upsertOffset(topic, group, message.offset())
                            .thenReturn(message);
                });
    }
}
