package com.github.opaluchlukasz.keyperdb.infrastructure

import com.github.opaluchlukasz.keyperdb.infrastructure.index.DbIndex
import com.github.opaluchlukasz.keyperdb.infrastructure.io.DbStorage
import spock.lang.Specification

import static java.util.UUID.randomUUID
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat

class CompactingJobTest extends Specification {
    private static final List<String> TEN_KEYS = (1..10).collect { randomUUID() as String } as List

    def 'should compact pages when page contains mostly duplicates'() {
        given:
        DbStorage dbStorage = randomDb()

        when:
        DbIndex dbIndex = populateDb(dbStorage, TEN_KEYS)

        then:
        assertThat(dbStorage.listPages()).hasSize(12)

        when:
        new CompactingJob(dbIndex, dbStorage).run()

        then:
        assertThat(dbStorage.listPages()).containsExactlyInAnyOrder('0000000012.seg', '0000000011.seg')
    }

    def 'should compact pages when page contains duplicates followed by unique keys'() {
        given:
        String firstPage = '0000000011.seg'
        DbStorage dbStorage = randomDb()

        when:
        populateDb(dbStorage, TEN_KEYS)
        DbIndex dbIndex = populateDbWithRandomData(dbStorage, 2_500)

        then:
        assertThat(dbStorage.listPages()).hasSize(15)

        when:
        new CompactingJob(dbIndex, dbStorage).run()

        then:
        assertThat(dbStorage.listPages())
                .containsExactlyInAnyOrder('0000000015.seg', '0000000014.seg', '0000000013.seg', firstPage)
        def firstPageIndex = dbIndex.getPageIndex(firstPage)
        TEN_KEYS.each {
            OptionalLong offset = firstPageIndex.offsetOf(it)
            assertThat(offset).isNotEmpty()
            Entry entryFromPageIndex = dbStorage.readEntryAtPageWithOffset(firstPage, offset.getAsLong())
            Optional<String> valueFromGlobalIndex = dbIndex.getIndexed(it)
            assertThat(valueFromGlobalIndex).isNotEmpty()
            assertThat(entryFromPageIndex.value()).isEqualTo(valueFromGlobalIndex.get())
        }
    }

    private static DbStorage randomDb() {
        String name = "target/temp/${randomUUID()}"
        return new DbStorage(name)
    }

    private static DbIndex populateDb(DbStorage dbStorage, List<String> keys) {
        DbIndex dbIndex = new DbIndex(dbStorage)
        10_000.times {
            dbIndex.put(keys[it % keys.size()], randomUUID() as String)
        }
        dbIndex
    }

    private static DbIndex populateDbWithRandomData(DbStorage dbStorage, int numberOfRecords) {
        DbIndex dbIndex = new DbIndex(dbStorage)
        numberOfRecords.times {
            dbIndex.put(randomUUID() as String, randomUUID() as String)
        }
        dbIndex
    }
}
