package com.jstream.models;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class AppendOnlyLog {
    private final Path path;
    private FileChannel fileChannel;
    private final AtomicLong currentOffset = new AtomicLong(0);

    public AppendOnlyLog(String topic) {
        this.path = Paths.get("data", topic + ".log");
        try {
            Files.createDirectories(path.getParent());
            this.fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            log.info("Disk log file initialized at {}", path.toAbsolutePath());
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
            String logEntry = payload + "\n";
            ByteBuffer buffer = ByteBuffer.wrap(logEntry.getBytes());
            fileChannel.write(buffer);
            return currentOffset.incrementAndGet();
        } catch (IOException e) {
            log.error("Failed to write a message to disk", e);
            return -1;
        }
    }
}