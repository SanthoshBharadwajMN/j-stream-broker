package com.jstream.models;

import com.jstream.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class AppendOnlyLog {
    private String topic;
    private final Path dataDir;
    private FileChannel currentChannel;

    private AtomicLong currentOffset = new AtomicLong(0);
    private int currentSegmentIndex = 0;
    private int messagesInCurrentSegment = 0;

    private static final int MAX_MESSAGES_PER_SEGMENT = 50;

    public AppendOnlyLog(String topic) {
        this.topic = topic;
        this.dataDir = Paths.get("data");
        try {
            Files.createDirectories(dataDir);
            initialize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initialize() throws IOException {
        List<Path> segmentFiles = getSegmentFiles();

        if (segmentFiles.isEmpty()) {
            // New topic, no existing files
            openChannel(0);
            return;
        }

        Path lastSegment = segmentFiles.get(segmentFiles.size() - 1);
        this.currentSegmentIndex = LogUtils.extractSegmentIndex(lastSegment);

        try (Stream<String> lines = Files.lines(lastSegment)) {
            List<String> allLines = lines.toList();
            this.messagesInCurrentSegment = allLines.size();

            if (!allLines.isEmpty()) {
                String lastLine = allLines.getLast();
                String lastOffset = lastLine.substring(0, lastLine.indexOf(":"));
                this.currentOffset.set(Long.parseLong(lastOffset));
            }
        }
        openChannel(this.currentSegmentIndex);
    }

    /**
     * Appends a message to the physical disk
     * Returns the offset ID of the message
     */
    public synchronized long append(String payload) {
        try {
            if (messagesInCurrentSegment >= MAX_MESSAGES_PER_SEGMENT) {
                log.info("[AppendOnlyLog] Segment {} is full for topic {}, creating segment {}", currentSegmentIndex, topic, currentSegmentIndex+1);
                messagesInCurrentSegment = 0;
                openChannel(++currentSegmentIndex);
            }

            long offset = currentOffset.incrementAndGet();
            String logEntry = offset + ":" + payload + "\n";
            ByteBuffer buffer = ByteBuffer.wrap(logEntry.getBytes());
            currentChannel.write(buffer);
            messagesInCurrentSegment++;
            return offset;
        } catch (IOException e) {
            log.error("Failed to write a message to disk", e);
            return -1;
        }
    }

    public Flux<BrokerMessage> replaySince(long lastReadOffset) {
        List<Path> segmentFiles;
        try {
            segmentFiles = getSegmentFiles();
        } catch (IOException e) {
            log.error("Exception while getting segment files for replay, e: " + e);
            return Flux.empty();
        }

        if (segmentFiles.isEmpty()) {
            return Flux.empty();
        }

        return Flux.fromIterable(segmentFiles)
                .concatMap(path -> {
                    return Flux.using(
                            () -> Files.lines(path),
                            stream -> {
                                return Flux.fromStream(stream)
                                        .map(line -> {
                                            int seperatorIndex = line.indexOf(":");
                                            long offset = Long.parseLong(line.substring(0, seperatorIndex));
                                            String payload = line.substring(seperatorIndex+1);
                                            return new BrokerMessage(offset, payload);
                                        })
                                        .filter(message -> message.offset() > lastReadOffset);
                            },
                            BaseStream::close
                    );
                });
    }

    private List<Path> getSegmentFiles() throws IOException {
        try (Stream<Path> pathStream = Files.list(dataDir)) {
            return pathStream
                    .filter(path -> path.getFileName().toString().startsWith(topic + "_") && path.getFileName().toString().endsWith(".log"))
                    .sorted(Comparator.comparingInt(LogUtils::extractSegmentIndex))
                    .collect(Collectors.toList());
        }
    }

    private void openChannel(int segmentIndex) throws IOException {
        if (this.currentChannel != null && this.currentChannel.isOpen()) {
            this.currentChannel.close();
        }
        Path path = dataDir.resolve(this.topic + "_" + segmentIndex + ".log");
        this.currentChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        log.info("[AppendOnlyLog] Opened file channel at " + path.toAbsolutePath());
    }
}