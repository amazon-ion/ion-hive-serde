package software.amazon.ionhiveserde.configuration

import org.junit.Test
import software.amazon.ion.IonInt
import software.amazon.ion.system.IonReaderBuilder
import software.amazon.ionhiveserde.ION
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

        val pathExtractor = PathExtractionConfig(
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