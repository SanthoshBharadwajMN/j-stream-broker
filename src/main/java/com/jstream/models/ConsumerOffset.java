package com.jstream.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table("consumer_offsets")
public class ConsumerOffset {
    @Id
    private Long id;
    private String topicName;
    private String consumerGroup;
    private Long currentOffset;
    private LocalDateTime updatedAt;

    public ConsumerOffset(String topicName, String consumerGroup, Long currentOffset) {
        this.topicName = topicName;
        this.consumerGroup = consumerGroup;
        this.currentOffset = currentOffset;
    }
}
