// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest.tests

import com.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import com.amazon.ionhiveserde.integrationtest.mkdir
import com.amazon.ionhiveserde.integrationtest.newBinaryWriterFromPath
import com.amazon.ionhiveserde.integrationtest.rm
import com.amazon.ionhiveserde.integrationtest.writeValues
import junitparams.JUnitParamsRunner
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hive.service.cli.HiveSQLException
import org.junit.Test
import org.junit.runner.RunWith
import java.io.FileWriter
import kotlin.reflect.KClass
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@RunWith(JUnitParamsRunner::class)
class IgnoreMalformedTest : com.amazon.ionhiveserde.integrationtest.Base() {
    companion object : com.amazon.ionhiveserde.integrationtest.TestLifecycle {
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
            inputFormatClass: KClass<*> = com.amazon.ionhiveserde.formats.IonInputFormat::class) {
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
