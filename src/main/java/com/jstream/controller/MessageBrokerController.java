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
        log.info("[Controller] Received message from producer: {}", payload);
        long offset = storageService.append(topic, payload);
        BrokerMessage message = new BrokerMessage(offset, payload);
        topicService.publish(topic, message);
        return Mono.empty();
    }

    @MessageMapping("subscribe.{topic}.{group}")
    public Flux<BrokerMessage> subscribeToTopic(@DestinationVariable("topic") String topic,
                                                @DestinationVariable("group") String group,
                                                Flux<Long> clientAcks
    ) {
        clientAcks.flatMap(ackedOffset -> {
            log.info("[Controller] Received ACK from [{}] for offset {}", group, ackedOffset);
            // Update the database only when the consumer provides ACK
            return consumerOffsetRepository.upsertOffset(topic, group, ackedOffset);
        }).subscribe(); // Listen to ACKs

        /*
        If a consumer group disconnects due to an issue at some point and reconnects a while later, fetch the last
        read offset from the database, stream the messages from that offset. Combine this with the live stream of messages
        from the sink
         */
        return consumerOffsetRepository.findByTopicNameAndConsumerGroup(topic, group)
                .map(consumerOffset -> consumerOffset.getCurrentOffset())
                .defaultIfEmpty(0L)
                .flatMapMany(lastOffset -> {
                    log.info("[Controller] Consumer [{}] last read offset was {}. Initiating replay", group, lastOffset);
                    Flux<BrokerMessage> oldBrokerMessages = storageService.replaySince(topic, lastOffset);
                    Flux<BrokerMessage> liveBrokerMessages = topicService.subscribe(topic)
                            .filter(liveMessage -> liveMessage.offset() > lastOffset);
                    return Flux.concat(oldBrokerMessages, liveBrokerMessages);
                });
    }
}
