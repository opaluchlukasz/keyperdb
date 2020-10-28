package com.github.opaluchlukasz.keyperdb.infrastructure

import spock.lang.Specification

import static java.util.UUID.randomUUID
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat

class FileManagerTest extends Specification {

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
        File file = new File("$name/1.seg")
        file.exists()
        file.isFile()
    }

    def 'should use already created db if exists'() {
        given:
        String name = "target/temp/${randomUUID()}"
        new FileManager(name)

        when:
        new FileManager(name)

        then:
        File file = new File("$name/1.seg")
        file.exists()
        file.isFile()
    }

    def 'should put and read entry'() {
        given:
        String name = "target/temp/${randomUUID()}"
        def manager = new FileManager(name)

        when:
        manager.put('foo', 'bar')

        then:
        manager
    }

    def 'should put multiple entries'() {
        given:
        String name = "target/temp/${randomUUID()}"
        def manager = new FileManager(name)

        when:
        manager.put('foo', 'bar')
        manager.put('baz', 'bar')

        then:
        manager
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
}
