package com.jstream.service;

import com.jstream.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.jstream.utils.LogUtils.extractSegmentIndex;

@Slf4j
@Service
public class LogDeletionService {
    private final Path dataDir = Paths.get("data");
    private static final int RETENTION_DAYS = 7;

    @Scheduled(cron = "0 0 0 * * *") // Runs every midnight
    public void deleteOldSegments() {
        if (!Files.exists(dataDir)) {
            return;
        }

        log.info("[LogDeletion] Waking up");
        Instant expirationThreshold = Instant.now().minus(RETENTION_DAYS, ChronoUnit.DAYS);

        try (Stream<Path> paths = Files.list(dataDir)) {
            Map<String, List<Path>> filesByTopic = paths
                    .filter(p -> p.toString().endsWith(".log"))
                    .collect(Collectors.groupingBy(LogUtils::extractTopicName));

            for (Map.Entry<String, List<Path>> entry : filesByTopic.entrySet()) {
                List<Path> topicFiles = entry.getValue();
                topicFiles.sort((p1, p2) -> {
                    return extractSegmentIndex(p1) - extractSegmentIndex(p2);
                });

                // Do not delete the last file
                if (topicFiles.size() > 1) {
                    List<Path> filesToBeDeleted = topicFiles.subList(0, topicFiles.size() - 1);
                    for (Path file : filesToBeDeleted) {
                        BasicFileAttributes fileAttributes = Files.readAttributes(file, BasicFileAttributes.class);
                        if (fileAttributes.lastModifiedTime().toInstant().isBefore(expirationThreshold)) {
                            log.info("[LogDeletion] Deleting expired log segment: {}", file.getFileName());
                            Files.delete(file);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("[LogDeletion] Exception while deleting older segments, e: " + e);
        }
    }
}
