package com.jstream.service;

import com.jstream.models.AppendOnlyLog;
import com.jstream.models.BrokerMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class StorageService {
    private final Map<String, AppendOnlyLog> topicLogs = new ConcurrentHashMap<>();

    public long append(String topic, String payload) {
        AppendOnlyLog appendOnlyLog = getOrCreateAppendOnlyLog(topic);
        return appendOnlyLog.append(payload);
    }

    public Flux<BrokerMessage> replaySince(String topic, long lastReadOffset) {
        AppendOnlyLog appendOnlyLog = getOrCreateAppendOnlyLog(topic);
        return appendOnlyLog.replaySince(lastReadOffset);
    }

    private AppendOnlyLog getOrCreateAppendOnlyLog(String topic) {
        return topicLogs.computeIfAbsent(topic, key -> {
            log.info("[StorageService] Creating AppendOnlyLog for topic {}", topic);
            return new AppendOnlyLog(topic);
        });
    }
}
