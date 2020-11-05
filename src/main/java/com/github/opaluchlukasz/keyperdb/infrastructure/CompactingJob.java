package com.github.opaluchlukasz.keyperdb.infrastructure;

import com.github.opaluchlukasz.keyperdb.infrastructure.index.DbIndex;
import com.github.opaluchlukasz.keyperdb.infrastructure.index.SinglePageIndex;
import com.github.opaluchlukasz.keyperdb.infrastructure.io.DbStorage;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class CompactingJob implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CompactingJob.class);

    private final DbIndex dbIndex;
    private final DbStorage dbStorage;

    public CompactingJob(DbIndex dbIndex, DbStorage dbStorage) {
        this.dbIndex = dbIndex;
        this.dbStorage = dbStorage;
    }

    @Override
    public void run() {
        try {
            List<String> pages = dbStorage.listPages().sorted().collect(toList());
            for (int i = 0; i + 1 < pages.size() - 1; i = i + 2) {
                String first = pages.get(i);
                String second = pages.get(i + 1);
                String temporarySecondPage = second.replace("seg", "tmp");
                String temporaryFirstPage = first.replace("seg", "tmp");

                LOG.info("Compacting pages: {}, {} into {}", first, second, temporarySecondPage);

                Path tempFirstPage = dbStorage.createPageFile(temporaryFirstPage);
                Path tempSecondPage = dbStorage.createPageFile(temporarySecondPage);

                rewriteToTempPage(first, tempFirstPage, tempSecondPage);
                rewriteToTempPage(second, tempFirstPage, tempSecondPage);

                buildIndexAndSwapFile(first, tempFirstPage);
                buildIndexAndSwapFile(second, tempSecondPage);
            }
        } catch (Exception ex) {
            LOG.warn("Compacting job failed", ex);
        }
    }

    private void buildIndexAndSwapFile(String originalPage, Path tempFirstPage) {
        String tempPageName = tempFirstPage.toFile().getName();
        if (tempFirstPage.toFile().length() == 0) {
            dbIndex.removePageIndex(originalPage);
            dbStorage.removePage(originalPage);
            dbStorage.removePage(tempPageName);
        } else {
            dbIndex.indexPage(tempPageName);
            dbStorage.replacePage(originalPage, tempPageName);
            dbIndex.indexPage(originalPage);
            dbIndex.removePageIndex(tempPageName);
            dbStorage.removePage(tempPageName);
        }
    }

    private void rewriteToTempPage(String page,
                                   Path tempPage,
                                   Path backupPage) {
        SinglePageIndex pageIndex = dbIndex.getPageIndex(page);
        RandomAccessFile originalPage = dbStorage.getPage(page);
        MutableLongCollection offsets = pageIndex.offsets();
        MutableLongIterator iterator = offsets.longIterator();

        Path currentPage = tempPage;

        while (iterator.hasNext()) {
            long offset = iterator.next();
            Entry entry = dbStorage.readEntryAtPageWithOffset(originalPage, offset);
            Optional<String> index = dbIndex.findPage(entry.key());
            if (index.isPresent() && page.equals(index.get())) {
                boolean exceedsPageSize = dbStorage.copyToTemporary(currentPage, entry.key(), entry.value());
                if (exceedsPageSize) {
                    currentPage = backupPage;
                }
            }
        }
    }
}
