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

package com.amazon.ionhiveserde.integrationtest.tests

import com.amazon.ion.IonDatagram
import com.amazon.ion.IonStruct
import com.amazon.ion.IonTimestamp
import com.amazon.ionhiveserde.integrationtest.*
import com.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import org.junit.Test
import kotlin.test.assertEquals

class TimestampSerializationOffsetTest : com.amazon.ionhiveserde.integrationtest.Base() {
    companion object : com.amazon.ionhiveserde.integrationtest.TestLifecycle {
        private const val TABLE_NAME = "TimestampSerializationOffsetTest"
        private const val TEST_DIR = "$SHARED_DIR/input/$TABLE_NAME/"
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
        private val serdeProperties = mapOf("ion.timestamp.serialization_offset" to "-08:00")

        override fun setup() {
            mkdir(TEST_DIR)

            newBinaryWriterFromPath("$TEST_DIR/timestamps.10n").use { it.writeValues(INPUT_TIMESTAMPS) }
        }

        override fun tearDown() {
            rm(TEST_DIR)
        }
    }

    private fun createTable() {
        hive().createExternalTable(
                TABLE_NAME,
                mapOf("field" to "TIMESTAMP"),
                "/data/input/$TABLE_NAME/",
                serdeProperties)
    }

    private fun IonDatagram.toTimestampList() = this.map { it as IonStruct }
            .map { it.first() as IonTimestamp }
            .map { it.timestampValue() }

    @Test
    fun timestampSerializedToConfiguredOffset() {
        createTable()

        val rawBytes = com.amazon.ionhiveserde.integrationtest.Base.hive().queryToFileAndRead("SELECT * FROM $TABLE_NAME", serdeProperties)
        val datagram = DOM_FACTORY.loader.load(rawBytes)

        val expectedOffset = -8 * 60

        val actualTimestamps = DOM_FACTORY.loader.load(INPUT_TIMESTAMPS).toTimestampList()

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
