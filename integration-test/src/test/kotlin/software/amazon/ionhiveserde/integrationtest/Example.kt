package software.amazon.ionhiveserde.integrationtest

import org.junit.Test
import software.amazon.ionhiveserde.integrationtest.hive.Hive
import kotlin.test.assertFalse

class Example : Base() {

    @Test
    fun name() {
        Hive().use { hive ->
            hive.query("show tables") { rs ->
                assertFalse(rs.next())
            }
        }
    }
}