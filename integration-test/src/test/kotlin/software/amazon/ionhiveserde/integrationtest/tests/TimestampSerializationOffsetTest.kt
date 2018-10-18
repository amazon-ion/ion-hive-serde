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

package software.amazon.ionhiveserde.integrationtest.tests

import org.junit.Test
import software.amazon.ion.IonDatagram
import software.amazon.ion.IonStruct
import software.amazon.ion.IonTimestamp
import software.amazon.ionhiveserde.integrationtest.*
import software.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import kotlin.test.assertEquals

class TimestampSerializationOffsetTest : Base() {
    companion object : TestLifecycle {
        private const val TEST_DIR = "$SHARED_DIR/input/TimestampSerializationOffsetTest/"
        private const val INPUT_TIMESTAMPS = """
            { field: 2018-01-01T10:10Z }
            { field: 2018-01-01T10:10-00:00 }
            { field: 2018-01-01T10:10+00:00 }
            { field: 2018-01-01T10:10-01:00 }
            { field: 2018-01-01T10:10+01:00 }
            { field: 2018-01-01T10:10+12:34 }
            { field: 2018-01-01T10:10-12:34 }
            { field: 2018-01-01T10:10-23:59 }
            { field: 2018-01-01T10:10+23:59 }
        """
        private val tableName = TimestampSerializationOffsetTest::javaClass.name
        private val serdeProperties = mapOf("timestamp.serialization_offset" to "-08:00")

        override fun setup() {
            mkdir(TEST_DIR)

            ION.newBinaryWriterFromPath("$TEST_DIR/timestamps.10n").use { writer ->
                ION.newReader(INPUT_TIMESTAMPS).use { writer.writeValues(it) }
            }
        }

        override fun tearDown() {
            rm(TEST_DIR)
        }
    }

    private fun createTable() {
        hive().createExternalTable(
                tableName,
                mapOf("field" to "TIMESTAMP"),
                "/data/input/TimestampSerializationOffsetTest/",
                serdeProperties)
    }

    private fun IonDatagram.toTimestampList() = this.map { it as IonStruct }
            .map { it.first() as IonTimestamp }
            .map { it.timestampValue() }

    @Test
    fun timestampSerializedToConfiguredOffset() {
        createTable()

        val rawBytes = Base.hive().queryToFileAndRead("SELECT * FROM $tableName", serdeProperties)
        val datagram = ION.loader.load(rawBytes)

        val expectedOffset = -8 * 60

        val actualTimestamps = ION.loader.load(INPUT_TIMESTAMPS).toTimestampList()

        assertEquals(actualTimestamps.size, datagram.size)
        datagram.toTimestampList().forEachIndexed { index, timestamp ->
            assertEquals(expectedOffset, timestamp.localOffset)

            val expectedTimestamp = actualTimestamps[index].withLocalOffset(expectedOffset)
            val assertErrorMessage = """|Expected: $expectedTimestamp
                                        |Actual:   $timestamp
                                        |""".trimMargin()

            assertEquals(expectedTimestamp.millis, timestamp.millis, message = assertErrorMessage)
        }
    }
}
