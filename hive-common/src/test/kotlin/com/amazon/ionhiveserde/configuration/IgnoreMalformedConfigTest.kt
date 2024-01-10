package com.amazon.ionhiveserde.configuration

import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class IgnoreMalformedConfigTest {
    private fun makeConfig(encoding: String) = IgnoreMalformedConfig(
        MapBasedRawConfiguration(mapOf("ion.ignore_malformed" to encoding))
    )

    @Test
    fun ignoreMalformedTrue() {
        val subject = makeConfig("true")

        assertTrue(subject.ignoreMalformed)
    }

    @Test
    fun ignoreMalformedFalse() {
        val subject = makeConfig("false")

        assertFalse(subject.ignoreMalformed)
    }

    @Test
    fun ignoreMalformedDefault() {
        val subject = IgnoreMalformedConfig(MapBasedRawConfiguration(mapOf()))

        assertFalse(subject.ignoreMalformed)
    }

    @Test
    fun ignoreMalformedNotBoolean() {
        val subject = makeConfig("not a boolean")

        assertFalse(subject.ignoreMalformed)
    }
}
