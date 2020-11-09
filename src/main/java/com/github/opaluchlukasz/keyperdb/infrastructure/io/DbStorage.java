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
import java.nio.file.StandardCopyOption;
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
            return readEntryAtPageWithOffset(pageFile, offset);
        } catch (IOException ex) {
            throw new DbException("Fatal exception while accessing page", ex);
        }
    }

    public Entry readEntryAtPageWithOffset(RandomAccessFile pageFile, long offset) {
        try {
            pageFile.seek(offset);
            return Entry.fromLine(pageFile.readLine());
        } catch (IOException ex) {
            throw new DbException("Fatal exception while accessing page", ex);
        }
    }

    public ObjectLongPair<String> put(Entry entry) {
        try {
            String page = lastPage.toFile().getName();
            long length = lastPage.toFile().length();
            Files.write(lastPage, entry.asBytes(), CREATE, APPEND);
            if (Files.size(lastPage) > PAGE_SIZE) {
                this.lastPage = createPageFile(pageFileName(nextPage()));
            }
            return PrimitiveTuples.pair(page, length);
        } catch (IOException ex) {
            throw new DbException("Fatal exception while appending record", ex);
        }
    }

    public boolean copyToTemporary(Path page, String key, String value) {
        try {
            Files.write(page, Entry.of(key, value).asBytes(), CREATE, APPEND);
            return page.toFile().length() > PAGE_SIZE;
        } catch (IOException ex) {
            throw new DbException("Fatal exception while putting record", ex);
        }
    }

    public Path createPageFile(String page) throws IOException {
        try {
            return Files.createFile(pagePath(page));
        } catch (FileAlreadyExistsException ex) {
            LOG.trace(format("Page %s already exists", page));
        }
        return pagePath(page);
    }

    private Path pagePath(String segment) {
        return Paths.get(dbPath.toFile().getPath(), segment);
    }

    private String pageFileName(long page) {
        return format("%010d.seg", page);
    }

    private Path lastPage() throws IOException {
        return pagePath(listPages().findFirst()
                .orElseThrow(() -> new RuntimeException("No page file found")));
    }

    private void createDbIfDoesNotExist() throws IOException {
        Files.createDirectories(dbPath);
        createPageFile(pageFileName(1));
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

    public void removePage(String originalPage) {
        try {
            Files.deleteIfExists(pagePath(originalPage));
        } catch (IOException ex) {
            throw new DbException("Fatal exception while accessing page", ex);
        }
    }

    public void replacePage(String originalPage, String toReplace) {
        try {
            Files.copy(pagePath(toReplace), pagePath(originalPage), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new DbException("Fatal exception while replacing page", ex);
        }
    }
}
