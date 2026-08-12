package com.jstream.service;

import com.jstream.models.AppendOnlyLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

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

    private AppendOnlyLog getOrCreateAppendOnlyLog(String topic) {
        return topicLogs.computeIfAbsent(topic, key -> {
            log.info("Creating AppendOnlyLog for topic {}", topic);
            return new AppendOnlyLog(topic);
        });
    }
}
