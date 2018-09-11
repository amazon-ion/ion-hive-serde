package software.amazon.ionhiveserde.integrationtest

import org.junit.BeforeClass
import software.amazon.ionhiveserde.integrationtest.docker.waitForHiveServer
import software.amazon.ionhiveserde.integrationtest.hive.Hive

abstract class Base {
    companion object {
        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            waitForHiveServer()
        }
    }
}