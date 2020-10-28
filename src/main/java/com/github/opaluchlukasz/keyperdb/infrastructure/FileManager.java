package com.github.opaluchlukasz.keyperdb.infrastructure;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class FileManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);
    private static final String ENTRY_SEPARATOR = "|:|";

    private final String name;

    public FileManager(String name) {
        try {
            this.name = name;
            createDbIfDoesNotExist();
        } catch (Exception ex) {
            throw new RuntimeException("Fatal exception", ex);
        }
    }

    private void createDbIfDoesNotExist() throws IOException {
        Files.createDirectories(Paths.get(name));
        createSegmentFile(1);
    }

    private void createSegmentFile(int i) throws IOException {
        try {
            Files.createFile(segmentPath(i));
        } catch (FileAlreadyExistsException ex) {
            LOG.trace(format("Segment %d already exists", i));
        }
    }

    private Path segmentPath(int i) {
        return Paths.get(name, format("%d.seg", i));
    }

    public void put(String key, String value) throws IOException {
        Files.write(segmentPath(1), (key + ENTRY_SEPARATOR + value + lineSeparator()).getBytes(UTF_8), CREATE, APPEND);
    }

    public Optional<String> get(String key) throws IOException {
        return findInSegment(key, 1);
    }

    private Optional<String> findInSegment(String key, int segment) throws IOException {
        try (ReversedLinesFileReader reverseReader = new ReversedLinesFileReader(segmentPath(segment).toFile(), UTF_8)) {
            String line = reverseReader.readLine();
            while (line != null) {
                String prefix = key + ENTRY_SEPARATOR;
                if (line.startsWith(prefix)) {
                    return Optional.of(line.substring(prefix.length()));
                }
                line = reverseReader.readLine();
            }
        }
        return Optional.empty();
    }
}
