package com.github.opaluchlukasz.keyperdb.infrastructure

import spock.lang.Specification

import static java.util.UUID.randomUUID
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat

class DbManagerTest extends Specification {
    private static final String FIRST_PAGE_FILE_NAME = '0000000001.seg'

    def 'should create db directory if it does not exist'() {
        given:
        String name = "target/temp/${randomUUID()}"

        when:
        new DbManager(name)

        then:
        File file = new File(name)
        file.exists()
        file.isDirectory()
    }

    def 'should create first page if it does not exist'() {
        given:
        String name = "target/temp/${randomUUID()}"

        when:
        new DbManager(name)

        then:
        File file = new File("$name/$FIRST_PAGE_FILE_NAME")
        file.exists()
        file.isFile()
    }

    def 'should use already created db if exists'() {
        given:
        String name = "target/temp/${randomUUID()}"
        DbManager manager = new DbManager(name)
        manager.put('foo', 'bar')

        when:
        manager = new DbManager(name)
        def value = manager.get('foo')

        then:
        assertThat(value).isPresent()
    }

    def 'should put multiple entries'() {
        given:
        String name = "target/temp/${randomUUID()}"
        def manager = new DbManager(name)

        manager.put('foo', 'bar')
        manager.put('baz', 'bar')

        when:
        Optional<String> value1 = manager.get('foo')

        then:
        assertThat(value1).isPresent()

        when:
        Optional<String> value2 = manager.get('baz')

        then:
        assertThat(value2).isPresent()
    }

    def 'should return proper value when key was overridden'() {
        given:
        String name = "target/temp/${randomUUID()}"
        def overriddenValue = 'baz'
        def manager = new DbManager(name)

        manager.put('foo', 'bar')
        manager.put('foo', overriddenValue)

        when:
        Optional<String> value = manager.get('foo')

        then:
        assertThat(value).isPresent()
        assertThat(value).hasValue(overriddenValue)
    }

    def 'creates new page when threshold reached'() {
        given:
        String name = "target/temp/${randomUUID()}"
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
        String name = "target/temp/${randomUUID()}"
        def overriddenValue = 'baz'
        def manager = new DbManager(name)

        for (int i = 0; i < 1_000; i++) {
            manager.put('foo', randomUUID() as String)
        }
        manager.put('foo', overriddenValue)

        when:
        Optional<String> value = manager.get('foo')

        then:
        assertThat(value).isPresent()
        assertThat(value).hasValue(overriddenValue)
    }

    def 'finds value on first page when multiple pages'() {
        given:
        String name = "target/temp/${randomUUID()}"
        def value = 'baz'
        def manager = new DbManager(name)

        manager.put('foo', value)
        for (int i = 0; i < 1_000; i++) {
            manager.put(randomUUID() as String, randomUUID() as String)
        }

        when:
        Optional<String> optionalValue = manager.get('foo')

        then:
        assertThat(optionalValue).isPresent()
        assertThat(optionalValue).hasValue(value)
    }
}
