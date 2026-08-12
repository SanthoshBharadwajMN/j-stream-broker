package com.jstream.repository;

import com.jstream.models.ConsumerOffset;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface ConsumerOffsetRepository extends ReactiveCrudRepository<ConsumerOffset, Long> {
    Mono<ConsumerOffset> findByTopicNameAndConsumerGroup(String topicName, String consumerGroup);

    @Query("""
           INSERT INTO consumer_offsets (topic_name, consumer_group, current_offset, updated_at)
           VALUES (:topic, :group, :offset, CURRENT_TIMESTAMP)
           ON CONFLICT (topic_name, consumer_group)
           DO UPDATE SET current_offset = :offset, updated_at = CURRENT_TIMESTAMP 
            """)
    Mono<Void> upsertOffset(String topic, String group, Long offset);
}
