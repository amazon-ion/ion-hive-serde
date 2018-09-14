package software.amazon.ionhiveserde.integrationtest.docker

import software.amazon.ionhiveserde.integrationtest.hive.Hive
import java.net.ConnectException

private const val WAIT_INTERVAL: Long = 2_000 // 2 seconds

/**
 * waits for hive server container to be up and responding to jdbc connections
 *
 * @param waitFor seconds to wait for before throwing an error
 */
@JvmOverloads
fun waitForHiveServer(waitFor: Int = 120) {
    var waitTimeInMillis = waitFor * 1_000L
    var connected = false

    while (waitTimeInMillis > 0 && !connected) {
        try {
            Hive().use { it.query("show tables") }
            connected = true
        } catch (e: Exception) {
            Thread.sleep(WAIT_INTERVAL)
        }
        waitTimeInMillis -= WAIT_INTERVAL
    }

    // timeout
    if (!connected && waitTimeInMillis <= 0) {
        throw ConnectException("Timeout trying to connect to Hive server. Waited for: $waitFor seconds")
    }
}
