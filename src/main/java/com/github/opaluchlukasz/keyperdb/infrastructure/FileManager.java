package com.github.opaluchlukasz.keyperdb.infrastructure;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class FileManager {
    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);
    private static final long SEGMENT_SIZE = 64 * 1024;
    private static final String ENTRY_SEPARATOR = "|:|";

    private final String name;
    private final Path dbPath;
    private Path lastSegment;

    public FileManager(String name) {
        try {
            this.name = name;
            this.dbPath = createDbIfDoesNotExist();
            this.lastSegment = lastSegment();
        } catch (Exception ex) {
            throw new RuntimeException("Fatal exception", ex);
        }
    }

    private Path lastSegment() throws IOException {
        return segmentPath(listSegments().findFirst()
                .orElseThrow(() -> new RuntimeException("No segment file found")));
    }

    private Path createDbIfDoesNotExist() throws IOException {
        Path databasePath = Paths.get(name);
        Files.createDirectories(databasePath);
        createSegmentFile(segmentFileName(1));
        return databasePath;
    }

    private void createSegmentFile(String segment) throws IOException {
        try {
            Files.createFile(segmentPath(segment));
        } catch (FileAlreadyExistsException ex) {
            LOG.trace(format("Segment %s already exists", segment));
        }
    }

    private Path segmentPath(String segment) {
        return Paths.get(name, segment);
    }

    private String segmentFileName(long segment) {
        return format("%010d.seg", segment);
    }

    public void put(String key, String value) throws IOException {
        Files.write(lastSegment, (key + ENTRY_SEPARATOR + value + lineSeparator()).getBytes(UTF_8), CREATE, APPEND);
        if (Files.size(lastSegment) > SEGMENT_SIZE) {
            createSegmentFile(segmentFileName(nextSegment()));
            this.lastSegment = lastSegment();
        }
    }

    private Long nextSegment() {
        return Long.parseLong(lastSegment.toFile().getName().replaceFirst(".seg", "")) + 1;
    }

    public Optional<String> get(String key) throws IOException {
        return listSegments()
                .flatMap(segment -> findInSegment(key, segment).stream())
                .findFirst();
    }

    private Stream<String> listSegments() throws IOException {
        return Files.list(dbPath)
                .map(path -> path.toFile().getName())
                .sorted(Comparator.comparing(Object::toString).reversed())
                .filter(name -> name.endsWith(".seg"));
    }

    private Optional<String> findInSegment(String key, String segment) {
        try (ReversedLinesFileReader reverseReader = new ReversedLinesFileReader(segmentPath(segment).toFile(), UTF_8)) {
            String line = reverseReader.readLine();
            while (line != null) {
                String prefix = key + ENTRY_SEPARATOR;
                if (line.startsWith(prefix)) {
                    return Optional.of(line.substring(prefix.length()));
                }
                line = reverseReader.readLine();
            }
        } catch (IOException ex) {
            LOG.error(format("Error while reading segment: %s", segment), ex);
            throw new RuntimeException(ex);
        }
        return Optional.empty();
    }
}
