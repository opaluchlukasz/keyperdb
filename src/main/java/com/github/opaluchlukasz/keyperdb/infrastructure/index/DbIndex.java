package com.github.opaluchlukasz.keyperdb.infrastructure.index;

import com.github.opaluchlukasz.keyperdb.infrastructure.Entry;
import com.github.opaluchlukasz.keyperdb.infrastructure.io.DbStorage;
import org.eclipse.collections.api.tuple.primitive.ObjectLongPair;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.Function;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;

public class DbIndex {
    private final DbStorage dbStorage;
    private final Map<String, SinglePageIndex> index;

    public DbIndex(DbStorage dbStorage) throws IOException {
        this.dbStorage = dbStorage;
        index = buildIndex();
    }

    public Optional<String> getIndexed(String key) {
        for (Map.Entry<String, SinglePageIndex> pageIndex : index.entrySet()) {
            OptionalLong offset = pageIndex.getValue().offsetOf(key);
            if (offset.isPresent()) {
                Entry entry = dbStorage.readEntryAtPageWithOffset(pageIndex.getKey(), offset.getAsLong());
                return entry.isDeleted() ? Optional.empty() : Optional.of(entry.value());
            }
        }
        return Optional.empty();
    }

    public void put(String key, String value) {
        store(Entry.of(key, value));
    }

    public void delete(String key) {
        store(Entry.deleted(key));
    }

    public Optional<String> findPage(String key) {
        for (Map.Entry<String, SinglePageIndex> pageIndex : index.entrySet()) {
            OptionalLong offset = pageIndex.getValue().offsetOf(key);
            if (offset.isPresent()) {
                return Optional.ofNullable(pageIndex.getKey());
            }
        }
        return Optional.empty();
    }

    private Map<String, SinglePageIndex> buildIndex() throws IOException {
        return dbStorage
                .listPages()
                .collect(toMap(
                    Function.identity(),
                    page -> new SinglePageIndex(dbStorage.getPage(page)),
                    (a, b) -> a,
                    () -> new TreeMap<>(reverseOrder())));
    }

    private void store(Entry entry) {
        ObjectLongPair<String> location = dbStorage.put(entry);
        String page = location.getOne();
        SinglePageIndex singlePageIndex = index.get(page);
        if (singlePageIndex == null) {
            indexPage(page);
        } else {
            singlePageIndex.index(entry.key(), location.getTwo());
        }
    }

    public SinglePageIndex getPageIndex(String page) {
        return index.get(page);
    }

    public void removePageIndex(String page) {
        index.remove(page);
    }

    public void indexPage(String pageName) {
        index.put(pageName, new SinglePageIndex(dbStorage.getPage(pageName)));
    }
}
