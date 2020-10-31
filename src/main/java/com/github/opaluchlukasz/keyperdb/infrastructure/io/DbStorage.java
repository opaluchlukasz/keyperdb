package com.github.opaluchlukasz.keyperdb.infrastructure.io;

import com.github.opaluchlukasz.keyperdb.api.DbException;
import com.github.opaluchlukasz.keyperdb.infrastructure.Entry;
import org.eclipse.collections.api.tuple.primitive.ObjectLongPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class DbStorage {
    private static final Logger LOG = LoggerFactory.getLogger(DbStorage.class);

    private static final String READ_MODE = "r";
    private static final long PAGE_SIZE = 64 * 1024;

    private final Path dbPath;
    private Path lastPage;

    public DbStorage(String name) throws IOException {
        this.dbPath = Paths.get(name);
        createDbIfDoesNotExist();
        this.lastPage = lastPage();
    }

    public Stream<String> listPages() throws IOException {
        return Files.list(dbPath)
                .map(path -> path.toFile().getName())
                .sorted(Comparator.comparing(Object::toString).reversed())
                .filter(name -> name.endsWith(".seg"));
    }

    public Entry readEntryAtPageWithOffset(String page, long offset) {
        try {
            RandomAccessFile pageFile = new RandomAccessFile(pagePath(page).toFile(), READ_MODE);
            pageFile.skipBytes((int) offset);
            return Entry.fromLine(pageFile.readLine());
        } catch (IOException ex) {
            throw new DbException("Fatal exception while accessing page", ex);
        }
    }

    public ObjectLongPair<String> put(String key, String value) {
        try {
            String page = lastPage.toFile().getName();
            long length = lastPage.toFile().length();
            Files.write(lastPage, Entry.of(key, value).asBytes(), CREATE, APPEND);
            if (Files.size(lastPage) > PAGE_SIZE) {
                createSegmentFile(pageFileName(nextPage()));
                this.lastPage = lastPage();
            }
            return PrimitiveTuples.pair(page, length);
        } catch (IOException ex) {
            throw new DbException("Fatal exception while putting record", ex);
        }
    }

    private void createSegmentFile(String segment) throws IOException {
        try {
            Files.createFile(pagePath(segment));
        } catch (FileAlreadyExistsException ex) {
            LOG.trace(format("Page %s already exists", segment));
        }
    }

    private Path pagePath(String segment) {
        return Paths.get(dbPath.toFile().getPath(), segment);
    }

    private String pageFileName(long segment) {
        return format("%010d.seg", segment);
    }

    private Path lastPage() throws IOException {
        return pagePath(listPages().findFirst()
                .orElseThrow(() -> new RuntimeException("No page file found")));
    }

    private void createDbIfDoesNotExist() throws IOException {
        Files.createDirectories(dbPath);
        createSegmentFile(pageFileName(1));
    }

    public RandomAccessFile getPage(String page) {
        try {
            return new RandomAccessFile(pagePath(page).toFile(), READ_MODE);
        } catch (IOException ex) {
            throw new DbException("Fatal exception while accessing page", ex);
        }
    }

    private Long nextPage() {
        return Long.parseLong(lastPage.toFile().getName().replaceFirst(".seg", "")) + 1;
    }
}
