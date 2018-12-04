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

import junitparams.JUnitParamsRunner
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonInt
import software.amazon.ion.IonStruct
import software.amazon.ion.system.IonReaderBuilder
import software.amazon.ionhiveserde.integrationtest.*
import software.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class PathExtractorTest : Base() {
    companion object : TestLifecycle {
        private const val TABLE_NAME = "PathExtractorTest"
        private const val TEST_DIR = "$SHARED_DIR/input/$TABLE_NAME/"

        private const val INPUT = """
            { field1: 1, obj: { nested: 11 } }
        """

        override fun setup() {
            mkdir(TEST_DIR)

            newBinaryWriterFromPath("$TEST_DIR/file.10n").use { it.writeValues(INPUT) }
        }

        override fun tearDown() {
            rm(TEST_DIR)
        }
    }

    private fun createTable(columns: Map<String, String>, serdeProperties: Map<String, String>) {
        hive().createExternalTable(
                TABLE_NAME,
                columns,
                "/data/input/$TABLE_NAME/",
                serdeProperties)
    }

    @Test
    fun pathExtractorRenameField() {
        val serdeProperties = mapOf("renamed_field1.path_extractor" to "(field1)")
        createTable(mapOf("renamed_field1" to "INT"), serdeProperties)

        val rawBytes = hive().queryToFileAndRead("SELECT renamed_field1 FROM $TABLE_NAME", serdeProperties)
        val datagram = DOM_FACTORY.loader.load(rawBytes)

        assertEquals(1, datagram.size)
        val struct = datagram[0] as IonStruct

        assertEquals(1, struct.size())
        val actual = struct.first() as IonInt
        assertEquals(1, actual.intValue())
    }

    @Test
    fun pathExtractorUnnest() {
        val serdeProperties = mapOf("field.path_extractor" to "(obj nested)")
        createTable(mapOf("field" to "INT"), serdeProperties)

        val rawBytes = hive().queryToFileAndRead("SELECT field FROM $TABLE_NAME", serdeProperties)
        val datagram = DOM_FACTORY.loader.load(rawBytes)

        assertEquals(1, datagram.size)
        val struct = datagram[0] as IonStruct

        assertEquals(1, struct.size())
        val actual = struct.first() as IonInt
        assertEquals(11, actual.intValue())
    }
}
