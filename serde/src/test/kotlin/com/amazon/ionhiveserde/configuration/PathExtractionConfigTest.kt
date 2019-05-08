package com.amazon.ionhiveserde.configuration

import com.amazon.ion.IonInt
import com.amazon.ion.system.IonReaderBuilder
import com.amazon.ionhiveserde.ION
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PathExtractionConfigTest {
    @Test
    fun pathExtractor() {
        val ionDocument = "{f1: 1, obj: {f2: 2}}"

        val struct = ION.newEmptyStruct()

        val configMap = mapOf(
                "ion.c1.path_extractor" to "(f1)",
                "ion.c2.path_extractor" to "(obj f2)")

        val pathExtractor = com.amazon.ionhiveserde.configuration.PathExtractionConfig(
                MapBasedRawConfiguration(configMap),
                listOf("c1", "c2")
        ).buildPathExtractor(struct, ION)

        assertTrue(struct.isEmpty)

        pathExtractor.match(IonReaderBuilder.standard().build(ionDocument))

        assertEquals(2, struct.size())
        assertEquals(1, (struct["c1"] as IonInt).intValue())
        assertEquals(2, (struct["c2"] as IonInt).intValue())
    }
}