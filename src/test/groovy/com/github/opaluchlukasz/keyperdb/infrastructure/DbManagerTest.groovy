package com.github.opaluchlukasz.keyperdb.infrastructure

import spock.lang.Specification

import static java.util.UUID.randomUUID
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat

class DbManagerTest extends Specification {
    private static final String FIRST_PAGE_FILE_NAME = '0000000001.seg'
    private static final String KEY = 'foo'

    def 'should create db directory if it does not exist'() {
        given:
        String name = randomDbPath()

        when:
        new DbManager(name)

        then:
        File file = new File(name)
        file.exists()
        file.isDirectory()
    }

    def 'should create first page if it does not exist'() {
        given:
        String name = randomDbPath()

        when:
        new DbManager(name)

        then:
        File file = new File("$name/$FIRST_PAGE_FILE_NAME")
        file.exists()
        file.isFile()
    }

    def 'should use already created db if exists'() {
        given:
        String name = randomDbPath()
        def manager = new DbManager(name)
        manager.put(KEY, 'bar')

        when:
        manager = new DbManager(name)
        def value = manager.get(KEY)

        then:
        assertThat(value).isPresent()
    }

    def 'should put multiple entries'() {
        given:
        def manager = givenRandomDbManager()

        manager.put(KEY, 'bar')
        manager.put('baz', 'bar')

        when:
        Optional<String> value1 = manager.get(KEY)

        then:
        assertThat(value1).isPresent()

        when:
        Optional<String> value2 = manager.get('baz')

        then:
        assertThat(value2).isPresent()
    }

    def 'should return proper value when key was overridden'() {
        given:
        def manager = givenRandomDbManager()
        def overriddenValue = 'baz'

        manager.put(KEY, 'bar')
        manager.put(KEY, overriddenValue)

        when:
        Optional<String> value = manager.get(KEY)

        then:
        assertThat(value).isPresent()
        assertThat(value).hasValue(overriddenValue)
    }

    def 'creates new page when threshold reached'() {
        given:
        String name = randomDbPath()
        def manager = new DbManager(name)

        for (int i = 0; i < 1_000; i++) {
            manager.put(randomUUID() as String, randomUUID() as String)
        }

        expect:
        File file = new File("$name/0000000002.seg")
        file.exists()
        file.isFile()
    }

    def 'finds last value when multiple pages'() {
        given:
        def manager = givenRandomDbManager()
        def overriddenValue = 'baz'

        for (int i = 0; i < 1_000; i++) {
            manager.put(KEY, randomUUID() as String)
        }
        manager.put(KEY, overriddenValue)

        when:
        Optional<String> value = manager.get(KEY)

        then:
        assertThat(value).isPresent()
        assertThat(value).hasValue(overriddenValue)
    }

    def 'finds value on first page when multiple pages'() {
        given:
        def manager = givenRandomDbManager()
        def value = 'baz'

        manager.put(KEY, value)
        for (int i = 0; i < 1_000; i++) {
            manager.put(randomUUID() as String, randomUUID() as String)
        }

        when:
        Optional<String> optionalValue = manager.get(KEY)

        then:
        assertThat(optionalValue).isPresent()
        assertThat(optionalValue).hasValue(value)
    }

    def 'deletes existing key'() {
        given:
        def manager = givenRandomDbManager()
        manager.put(KEY, randomUUID().toString())

        when:
        manager.delete(KEY)

        then:
        Optional<String> optionalValue = manager.get(KEY)
        assertThat(optionalValue).isEmpty()
    }

    def 'deletes non-existent key'() {
        given:
        def manager = givenRandomDbManager()

        when:
        manager.delete(KEY)

        then:
        Optional<String> optionalValue = manager.get(KEY)
        assertThat(optionalValue).isEmpty()
    }

    private static DbManager givenRandomDbManager() {
        String name = randomDbPath()
        new DbManager(name)
    }

    private static GString randomDbPath() {
        "target/temp/${randomUUID()}"
    }
}
