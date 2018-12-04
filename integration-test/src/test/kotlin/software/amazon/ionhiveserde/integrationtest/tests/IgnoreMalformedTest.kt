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
import org.apache.hive.service.cli.HiveSQLException
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ionhiveserde.integrationtest.*
import software.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import java.io.FileWriter
import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(JUnitParamsRunner::class)
class IgnoreMalformedTest : Base() {
    companion object : TestLifecycle {
        private const val TABLE_NAME = "IgnoreMalformedTest"
        private const val TEST_DIR = "$SHARED_DIR/input/$TABLE_NAME/"

        private const val VALID = "{ field: 1 }"
        private const val INVALID = "{ 1: not valid ion"
        private const val MIXED = "{ field: 2 }{ 1: not valid ion"

        override fun setup() {
            mkdir(TEST_DIR)
            mkdir("$TEST_DIR/separated")
            mkdir("$TEST_DIR/mixed")

            newBinaryWriterFromPath("$TEST_DIR/separated/valid.10n").use { it.writeValues(VALID) }
            FileWriter("$TEST_DIR/separated/invalid.txt").use { it.write(INVALID) }
            FileWriter("$TEST_DIR/mixed/mixed.txt").use { it.write(MIXED) }
        }

        override fun tearDown() {
            rm(TEST_DIR)
        }
    }

    private fun createTable(ignoreMalformed: Boolean, location: String) {
        hive().createExternalTable(
                TABLE_NAME,
                mapOf("field" to "INT"),
                "/data/input/$TABLE_NAME/$location",
                mapOf("ignore_malformed" to ignoreMalformed.toString()))
    }

    @Test
    fun ignoreMalformed() {
        createTable(ignoreMalformed = true, location = "separated")

        val rawBytes = hive().query("SELECT field FROM $TABLE_NAME") { rs ->
            assertTrue(rs.next())

            assertEquals(1, rs.getInt(1))

            assertFalse(rs.next())
        }
    }

    @Test
    fun ignoreMalformedMixed() {
        createTable(ignoreMalformed = true, location = "mixed")

        val rawBytes = hive().query("SELECT field FROM $TABLE_NAME") { rs ->
            assertTrue(rs.next())

            assertEquals(2, rs.getInt(1))

            assertFalse(rs.next())
        }
    }

    @Test(expected = HiveSQLException::class)
    fun doNotIgnoreMalformedMixed() {
        createTable(ignoreMalformed = false, location = "mixed")

        hive().query("SELECT field FROM $TABLE_NAME") { it.next() }
    }

    @Test(expected = HiveSQLException::class)
    fun doNotIgnoreMalformed() {
        createTable(ignoreMalformed = false, location = "separated")

        hive().query("SELECT field FROM $TABLE_NAME") { it.next() }
    }
}
