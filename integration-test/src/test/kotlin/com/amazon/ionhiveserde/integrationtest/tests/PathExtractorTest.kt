// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest.tests

import com.amazon.ion.IonInt
import com.amazon.ion.IonStruct
import com.amazon.ionhiveserde.integrationtest.*
import com.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import junitparams.JUnitParamsRunner
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class PathExtractorTest : com.amazon.ionhiveserde.integrationtest.Base() {
    companion object : com.amazon.ionhiveserde.integrationtest.TestLifecycle {
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
        val serdeProperties = mapOf("ion.renamed_field1.path_extractor" to "(field1)")
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
        val serdeProperties = mapOf("ion.field.path_extractor" to "(obj nested)")
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
