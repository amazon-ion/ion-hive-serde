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

import com.amazon.ion.IonStruct
import com.amazon.ion.IonText
import com.amazon.ion.impl._Private_Utils
import com.amazon.ion.system.IonReaderBuilder
import com.amazon.ion.system.SimpleCatalog
import com.amazon.ionhiveserde.integrationtest.DOM_FACTORY
import com.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import com.amazon.ionhiveserde.integrationtest.mkdir
import com.amazon.ionhiveserde.integrationtest.newBinaryWriterFromPath
import com.amazon.ionhiveserde.integrationtest.rm
import org.junit.Test
import java.io.File
import java.io.FileWriter
import kotlin.test.assertEquals

class SharedSymbolTableTest : com.amazon.ionhiveserde.integrationtest.Base() {
    companion object : com.amazon.ionhiveserde.integrationtest.TestLifecycle {
        private const val TABLE_NAME = "SharedSymbolTableTest"
        private const val TEST_DIR = "$SHARED_DIR/input/$TABLE_NAME"
        private const val CATALOG_PATH = "$SHARED_DIR/input/$TABLE_NAME/catalog.ion"
        private const val CATALOG = """
            ${'$'}ion_shared_symbol_table::{
                name: "sst",
                version: 1,
                symbols: [ "foo" ]
            }
        """

        private const val INPUT = """
            ${'$'}ion_symbol_table::{
                imports:[{
                    name: "sst",
                    version: 1,
                    max_id: 1
                }]
            }
            {field: foo}
        """

        override fun setup() {
            mkdir(TEST_DIR)
            mkdir("$TEST_DIR/data")

            FileWriter(File(CATALOG_PATH)).use { it.write(CATALOG) }

            @SuppressWarnings("deprecated")
            val sst = _Private_Utils.newSharedSymtab(IonReaderBuilder.standard().build(CATALOG), false)
            val catalog = SimpleCatalog().apply { putTable(sst) }

            newBinaryWriterFromPath("$TEST_DIR/data/input.10n", catalog, sst).use { writer ->
                IonReaderBuilder.standard().build(INPUT).use { reader ->
                    writer.writeValues(reader)
                }
            }
        }

        override fun tearDown() {
            rm(TEST_DIR)
        }
    }

    @Test
    fun sharedSymbolTableTest() {
        val serdeProperties = mapOf(
                "ion.catalog.file" to "/$CATALOG_PATH",
                "ion.symbol_table_imports" to "sst")

        hive().createExternalTable(
                TABLE_NAME,
                mapOf("field" to "STRING"),
                "/data/input/$TABLE_NAME/data/",
                serdeProperties)

        val rawBytes = hive().queryToFileAndRead("SELECT * FROM $TABLE_NAME", serdeProperties)
        val datagram = DOM_FACTORY.loader.load(rawBytes)

        assertEquals(1, datagram.size)
        assertEquals("foo", ((datagram[0] as IonStruct).first() as IonText).stringValue())
    }
}
