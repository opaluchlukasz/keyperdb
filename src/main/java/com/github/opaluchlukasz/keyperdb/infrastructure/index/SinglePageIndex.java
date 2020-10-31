package com.github.opaluchlukasz.keyperdb.infrastructure.index;

import com.github.opaluchlukasz.keyperdb.api.DbException;
import com.github.opaluchlukasz.keyperdb.infrastructure.Entry;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.OptionalLong;

public class SinglePageIndex {
    public static final int OFFSET_IF_NOT_FOUND = -1;

    private final ObjectLongHashMap<String> pageIndex;

    public SinglePageIndex(RandomAccessFile page) {
        pageIndex = new ObjectLongHashMap<>();
        try {
            long length = page.length();
            long filePointer = page.getFilePointer();
            while (filePointer < length) {
                Entry entry = Entry.fromLine(page.readLine());
                pageIndex.put(entry.key(), filePointer);
                filePointer = page.getFilePointer();
            }
        } catch (IOException ex) {
            throw new DbException("Fatal exception during building index", ex);
        }
    }

    public OptionalLong offsetOf(String key) {
        long entryOffset = pageIndex.getIfAbsent(key, OFFSET_IF_NOT_FOUND);
        if (entryOffset != OFFSET_IF_NOT_FOUND) {
            return OptionalLong.of(entryOffset);
        }
        return OptionalLong.empty();
    }

    public void index(String key, long offset) {
        pageIndex.put(key, offset);
    }
}
