package com.amazon.ionhiveserde.configuration

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class SerializeNullConfigTest {
    private fun makeConfig(serializeNull: String) = SerializeNullConfig(
        MapBasedRawConfiguration(mapOf("ion.serialize_null" to serializeNull))
    )

    private fun serializeNullOptions() = SerializeNullStrategy.values()

    @Test
    @Parameters(method = "serializeNullOptions")
    fun serializeNull(serializeNullStrategy: SerializeNullStrategy) {
        val subject = makeConfig(serializeNullStrategy.name)

        assertEquals(serializeNullStrategy, subject.serializeNull)
    }

    @Test
    fun defaultSerializeNull() {
        val subject = SerializeNullConfig(MapBasedRawConfiguration(mapOf()))

        assertEquals(SerializeNullStrategy.OMIT, subject.serializeNull)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidSerializeNull() {
        makeConfig("invalid option")
    }
}
