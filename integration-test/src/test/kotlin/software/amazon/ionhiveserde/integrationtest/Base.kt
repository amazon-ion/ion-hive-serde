package software.amazon.ionhiveserde.integrationtest

import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import software.amazon.ion.*
import software.amazon.ion.system.IonSystemBuilder
import software.amazon.ionhiveserde.integrationtest.docker.waitForHiveServer
import software.amazon.ionhiveserde.integrationtest.hive.Hive

/**
 * Base class for all integration tests. Handles the connection lifecycle to reuse the same connection as much as
 * possible.
 */
abstract class Base {
    companion object {
        @JvmStatic
        var configuration: TestConfiguration = NonSuiteConfiguration

        private var hive: Hive? = null

        @JvmStatic
        fun hive(): Hive {
            if (hive == null || hive!!.isClosed()) {
                hive = Hive()
            }

            return hive!!
        }

        @BeforeClass
        @JvmStatic
        fun beforeAll() {
            if (configuration.shouldWait) {
                waitForHiveServer()
            }
        }

        @AfterClass
        @JvmStatic
        fun afterAll() {
            hive().dropAllTables()

            if (configuration.shouldClose) {
                hive().close()
            }
        }
    }

    protected val ion: IonSystem = IonSystemBuilder.standard().build()

    @Before
    fun beforeEach() {
        hive().dropAllTables()
    }
}
