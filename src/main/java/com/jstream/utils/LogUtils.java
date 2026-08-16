package com.jstream.utils;

import java.nio.file.Path;

public class LogUtils {
    private LogUtils() {}

    /**
     * Extracts 'test' from test_3.log
     */
    public static String extractTopicName(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.substring(0, fileName.lastIndexOf("_"));
    }

    /**
     * Extract '3' from test_3.log
     */
    public static int extractSegmentIndex(Path path) {
        String fileName = path.getFileName().toString();
        String num = fileName.substring(fileName.lastIndexOf("_") + 1, fileName.lastIndexOf("."));
        return Integer.parseInt(num);
    }
}
