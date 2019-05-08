package com.amazon.ionhiveserde.configuration

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class SerializeNullConfigTest {
    private fun makeConfig(serializeNull: String) = com.amazon.ionhiveserde.configuration.SerializeNullConfig(
            MapBasedRawConfiguration(mapOf("ion.serialize_null" to serializeNull)))

    private fun serializeNullOptions() = com.amazon.ionhiveserde.configuration.SerializeNullStrategy.values()

    @Test
    @Parameters(method = "serializeNullOptions")
    fun serializeNull(serializeNullStrategy: com.amazon.ionhiveserde.configuration.SerializeNullStrategy) {
        val subject = makeConfig(serializeNullStrategy.name)

        assertEquals(serializeNullStrategy, subject.serializeNull)
    }

    @Test
    fun defaultSerializeNull() {
        val subject = com.amazon.ionhiveserde.configuration.SerializeNullConfig(MapBasedRawConfiguration(mapOf()))

        assertEquals(com.amazon.ionhiveserde.configuration.SerializeNullStrategy.OMIT, subject.serializeNull)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidSerializeNull() {
        makeConfig("invalid option")
    }
}