package com.amazon.ionhiveserde.configuration

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class EncodingConfigTest {
    private fun encodings() = IonEncoding.values()

    private fun makeConfig(encoding: String) = EncodingConfig(
        MapBasedRawConfiguration(mapOf("ion.encoding" to encoding))
    )

    @Test
    @Parameters(method = "encodings")
    fun encoding(encoding: IonEncoding) {
        val subject = makeConfig(encoding.name)

        assertEquals(encoding, subject.encoding)
    }

    @Test
    fun defaultEncoding() {
        val subject = EncodingConfig(MapBasedRawConfiguration(mapOf()))

        assertEquals(IonEncoding.BINARY, subject.encoding)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidEncoding() {
        makeConfig("not an encoding")
    }

}
