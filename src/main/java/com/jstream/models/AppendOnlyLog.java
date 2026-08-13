package com.jstream.models;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.BaseStream;

@Slf4j
public class AppendOnlyLog {
    private final Path path;
    private FileChannel fileChannel;
    private AtomicLong currentOffset = new AtomicLong(0);

    public AppendOnlyLog(String topic) {
        this.path = Paths.get("data", topic + ".log");
        try {
            Files.createDirectories(path.getParent());

            long startingOffset = 0;
            if (Files.exists(path)) {
                try (var lines = Files.lines(path)) {
                    startingOffset = lines.count();
                }
            }
            this.currentOffset = new AtomicLong(startingOffset);

            this.fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            log.info("[AppendOnlyLog] Disk log file initialized at {}", path.toAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Appends a message to the physical disk
     * Returns the offset ID of the message
     */
    public long append(String payload) {
        try {
            long offset = currentOffset.incrementAndGet();
            String logEntry = offset + ":" + payload + "\n";
            ByteBuffer buffer = ByteBuffer.wrap(logEntry.getBytes());
            fileChannel.write(buffer);
            log.debug("[AppendOnlyLog] Saved message '{}' to disk at offset {}", payload, offset);
            return offset;
        } catch (IOException e) {
            log.error("Failed to write a message to disk", e);
            return -1;
        }
    }

    public Flux<BrokerMessage> replaySince(long lastReadOffset) {
        if (!Files.exists(path)) {
            return Flux.empty();
        }
        return Flux.using(
                () -> Files.lines(this.path),
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
    }
}