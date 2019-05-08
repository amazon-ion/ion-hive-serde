/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package com.amazon.ionhiveserde.integrationtest.docker

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.net.ConnectException
import java.util.concurrent.TimeUnit

private const val HIVE_SERVER_WAIT_INTERVAL_MS: Long = 2_000 // 2 seconds

/**
 * Waits for hive server container to be up and responding to jdbc connections.
 *
 * @param waitFor seconds to wait for before throwing an error.
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
            Thread.sleep(HIVE_SERVER_WAIT_INTERVAL_MS)
        }
        waitTimeInMillis -= HIVE_SERVER_WAIT_INTERVAL_MS
    }

    // timeout
    if (!connected && waitTimeInMillis <= 0) {
        throw ConnectException("Timeout trying to connect to Hive server. Waited for: $waitFor seconds")
    }
}

private const val DEFAULT_SHELL_TIMEOUT_MS: Long = 60_000 // 60 seconds
private const val HIVE_CONTAINER_NAME = "hive-server"

/**
 * Encapsulates sending shell commands to docker container through docker-compose.
 *
 * @param command command to run.
 * @param timeoutInMs timeout in milliseconds, defaults to 60s.
 *
 * @throws ShellException if the command didn't return 0.
 */
fun runInDocker(command: String, timeoutInMs: Long = DEFAULT_SHELL_TIMEOUT_MS): String {
    fun read(stream: InputStream): String = BufferedReader(InputStreamReader(stream)).use { it.readText() }
    fun String.toCmdArray() = this.split(" ").toTypedArray()

    val process = Runtime.getRuntime().exec("docker exec $HIVE_CONTAINER_NAME $command".toCmdArray())

    val finished = process.waitFor(timeoutInMs, TimeUnit.MILLISECONDS)

    if (!finished) {
        throw ShellException("Command timed out: '$command'")
    }

    return when (process.exitValue()) {
        0    -> read(process.inputStream)
        else -> throw ShellException(read(process.errorStream))
    }
}

class ShellException(message: String) : Exception(message)