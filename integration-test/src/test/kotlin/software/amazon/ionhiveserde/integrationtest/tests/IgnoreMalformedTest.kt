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
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hive.service.cli.HiveSQLException
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ionhiveserde.formats.IonInputFormat
import software.amazon.ionhiveserde.integrationtest.*
import software.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import java.io.FileWriter
import java.io.IOException
import kotlin.reflect.KClass
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@RunWith(JUnitParamsRunner::class)
class IgnoreMalformedTest : Base() {
    companion object : TestLifecycle {
        private const val TABLE_NAME = "IgnoreMalformedTest"
        private const val TEST_DIR = "$SHARED_DIR/input/$TABLE_NAME/"

        private const val VALID = "{ field: 1 }"
        private const val INVALID = "{ 1: not valid ion"
        private const val MIXED = "{ field: 2 }{ 1: not valid ion"
        private const val ION_LINES = "{ field: 1 }\n{ 1: not valid ion\n{ field: 2 }" //JsonLines style

        override fun setup() {
            mkdir(TEST_DIR)
            mkdir("$TEST_DIR/separated")
            mkdir("$TEST_DIR/mixed")
            mkdir("$TEST_DIR/ionlines")

            newBinaryWriterFromPath("$TEST_DIR/separated/valid.10n").use { it.writeValues(VALID) }
            FileWriter("$TEST_DIR/separated/invalid.txt").use { it.write(INVALID) }
            FileWriter("$TEST_DIR/mixed/mixed.txt").use { it.write(MIXED) }
            FileWriter("$TEST_DIR/ionlines/ionlines.txt").use { it.write(ION_LINES) }
        }

        override fun tearDown() {
            rm(TEST_DIR)
        }
    }

    private fun createTable(
            ignoreMalformed: Boolean,
            location: String,
            inputFormatClass: KClass<*> = IonInputFormat::class) {
        hive().createExternalTable(
                tableName = TABLE_NAME,
                columns = mapOf("field" to "INT"),
                location = "/data/input/$TABLE_NAME/$location",
                serdeProperties = mapOf("ion.ignore_malformed" to ignoreMalformed.toString()),
                inputFormatClass = inputFormatClass)
    }

    @Test
    fun ignoreMalformed() {
        createTable(ignoreMalformed = true, location = "separated")

        hive().query("SELECT field FROM $TABLE_NAME") { rs ->
            assertTrue(rs.next())

            assertEquals(1, rs.getInt(1))

            assertFalse(rs.next())
        }
    }

    @Test
    fun ignoreMalformedMixed() {
        createTable(ignoreMalformed = true, location = "mixed")

        hive().query("SELECT field FROM $TABLE_NAME") { rs ->
            assertTrue(rs.next())

            assertEquals(2, rs.getInt(1))

            assertFalse(rs.next())
        }
    }

    @Test
    fun ignoreMalformedIonLines() {
        createTable(ignoreMalformed = true, location = "ionlines", inputFormatClass = TextInputFormat::class)

        hive().query("SELECT field FROM $TABLE_NAME") { rs ->
            assertTrue(rs.next())
            assertEquals(1, rs.getInt(1))

            // skipped line
            assertTrue(rs.next())
            assertNull(rs.getObject(1))

            assertTrue(rs.next())
            assertEquals(2, rs.getInt(1))

            assertFalse(rs.next())
        }
    }

    @Test(expected = HiveSQLException::class)
    fun doNotIgnoreMalformedMixed() {
        createTable(ignoreMalformed = false, location = "mixed")

        hive().query("SELECT field FROM $TABLE_NAME") { while(it.next()) {} }
    }

    @Test(expected = HiveSQLException::class)
    fun doNotIgnoreMalformed() {
        createTable(ignoreMalformed = false, location = "separated")

        hive().query("SELECT field FROM $TABLE_NAME") { while(it.next()) {} }
    }

    @Test(expected = HiveSQLException::class)
    fun doNotIgnoreMalformedIonLines() {
        createTable(ignoreMalformed = false, location = "ionlines", inputFormatClass = TextInputFormat::class)

        hive().query("SELECT field FROM $TABLE_NAME") { while(it.next()) {} }
    }
}
