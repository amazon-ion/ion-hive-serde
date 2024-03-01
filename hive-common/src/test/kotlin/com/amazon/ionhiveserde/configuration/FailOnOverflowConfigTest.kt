package com.amazon.ionhiveserde.configuration

import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class FailOnOverflowConfigTest {
    private fun makeConfig(map: Map<String, String>, columnNames: List<String>) =
        FailOnOverflowConfig(MapBasedRawConfiguration(map), columnNames)

    @Test
    fun failOnOverflow() {
        val subject = makeConfig(
                mapOf(
                        "ion.fail_on_overflow" to "false", // sets default
                        "ion.column_1.fail_on_overflow" to "true",
                        "ion.not_a_column.fail_on_overflow" to "true"
                ),
                listOf("column_1"))

        assertTrue(subject.failOnOverflowFor("column_1"))
        assertFalse(subject.failOnOverflowFor("not_a_column")) // not a column
        assertFalse(subject.failOnOverflowFor("not even in the config"))
    }

    @Test
    fun failOnOverflowDefault() {
        val subject = makeConfig(mapOf(), listOf("column_1"))

        assertTrue(subject.failOnOverflowFor("column_1"))
        assertTrue(subject.failOnOverflowFor("not_a_column"))
    }

    @Test
    fun failOnOverflowNotBoolean() {
        val subject = makeConfig(mapOf("ion.fail_on_overflow" to "not a boolean"), listOf("column_1"))

        assertFalse(subject.failOnOverflowFor("column_1"))
        assertFalse(subject.failOnOverflowFor("not_a_column")) // not a column
    }
}
