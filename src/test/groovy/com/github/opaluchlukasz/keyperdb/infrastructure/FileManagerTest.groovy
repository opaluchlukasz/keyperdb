package com.github.opaluchlukasz.keyperdb.infrastructure

import spock.lang.Specification

import static java.util.UUID.randomUUID
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat

class FileManagerTest extends Specification {
    private static final String FIRST_SEGMENT_FILE_NAME = '0000000001.seg'

    def 'should create db directory if it does not exist'() {
        given:
        String name = "target/temp/${randomUUID()}"

        when:
        new FileManager(name)

        then:
        File file = new File(name)
        file.exists()
        file.isDirectory()
    }

    def 'should create first segment file if it does not exist'() {
        given:
        String name = "target/temp/${randomUUID()}"

        when:
        new FileManager(name)

        then:
        File file = new File("$name/$FIRST_SEGMENT_FILE_NAME")
        file.exists()
        file.isFile()
    }

    def 'should use already created db if exists'() {
        given:
        String name = "target/temp/${randomUUID()}"
        FileManager manager = new FileManager(name)
        manager.put('foo', 'bar')

        when:
        manager = new FileManager(name)
        def value = manager.get('foo')

        then:
        assertThat(value).isPresent()
    }

    def 'should put multiple entries'() {
        given:
        String name = "target/temp/${randomUUID()}"
        def manager = new FileManager(name)

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
        def manager = new FileManager(name)

        manager.put('foo', 'bar')
        manager.put('foo', overriddenValue)

        when:
        Optional<String> value = manager.get('foo')

        then:
        assertThat(value).isPresent()
        assertThat(value).hasValue(overriddenValue)
    }

    def 'creates new segment when threshold reached'() {
        given:
        String name = "target/temp/${randomUUID()}"
        def manager = new FileManager(name)

        for (int i = 0; i < 1_000; i++) {
            manager.put(randomUUID() as String, randomUUID() as String)
        }

        expect:
        File file = new File("$name/0000000002.seg")
        file.exists()
        file.isFile()
    }

    def 'find last value when multiple segments'() {
        given:
        String name = "target/temp/${randomUUID()}"
        def overriddenValue = 'baz'
        def manager = new FileManager(name)

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
}
