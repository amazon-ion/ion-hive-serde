// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest.tests

import com.amazon.ion.IonStruct
import com.amazon.ion.IonText
import com.amazon.ion.IonType
import com.amazon.ion.IonValue
import com.amazon.ionhiveserde.integrationtest.*
import com.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class SerializeAsTest : com.amazon.ionhiveserde.integrationtest.Base() {
    companion object : com.amazon.ionhiveserde.integrationtest.TestLifecycle {
        private const val TEST_DIR = "$SHARED_DIR/input/SerializeAsTest/"
        private const val tableNamePrefix = "SerializeAsTest"

        private const val INPUT = """
            { hiveType: "STRING", ionType: "STRING", value: "text", expected: "text" }
            { hiveType: "STRING", ionType: "SYMBOL", value: "text", expected: 'text' }

            { hiveType: "CHAR(20)", ionType: "STRING", value: "text", expected: "text" }
            { hiveType: "CHAR(20)", ionType: "SYMBOL", value: "text", expected: 'text' }

            { hiveType: "VARCHAR(20)", ionType: "STRING", value: "text", expected: "text" }
            { hiveType: "VARCHAR(20)", ionType: "SYMBOL", value: "text", expected: 'text' }

            { hiveType: "DECIMAL", ionType: "DECIMAL", value: 1.0, expected: 1. }
            { hiveType: "DECIMAL", ionType: "INT",     value: 1.0, expected: 1 }

            { hiveType: "ARRAY<INT>", ionType: "LIST", value: [1,2,3], expected: [1,2,3] }
            { hiveType: "ARRAY<INT>", ionType: "SEXP", value: [1,2,3], expected: (1 2 3) }

            { hiveType: "BINARY", ionType: "BLOB", value: {{ "text" }}, expected: {{ dGV4dA== }} }
            { hiveType: "BINARY", ionType: "CLOB", value: {{ "text" }}, expected: {{ "text" }} }
        """

        data class TestCase(val hiveType: String, val ionType: String, val value: IonValue, val expected: IonValue)

        private fun parseInput() = DOM_FACTORY.loader.load(INPUT)
                .map { it as IonStruct }
                .map {
                    TestCase(
                            (it["hiveType"] as IonText).stringValue(),
                            (it["ionType"] as IonText).stringValue(),
                            it["value"],
                            it["expected"])
                }

        override fun setup() {
            mkdir(TEST_DIR)

            parseInput().forEach { testCase ->
                val path = "$TEST_DIR/${testCase.hiveType.sanitize()}_${testCase.ionType}"
                mkdir(path)

                newBinaryWriterFromPath("$path/file.10n").use { writer ->
                    writer.stepIn(IonType.STRUCT)
                    writer.setFieldName("field")
                    testCase.value.writeTo(writer)
                    writer.stepOut()
                }
            }
        }

        override fun tearDown() {
            rm(TEST_DIR)
        }
    }

    private fun serdeProperties(ionType: String) = mapOf("ion.column[0].serialize_as" to ionType)
    private fun tableName(hiveType: String, ionType: String): String =
            "${tableNamePrefix}_${hiveType.sanitize()}_${ionType.sanitize()}"

    private fun createTable(tableName: String, hiveType: String, ionType: String, serdeProperties: Map<String, String>) {
        hive().createExternalTable(
                tableName,
                mapOf("field" to hiveType),
                "/data/input/SerializeAsTest/${hiveType.sanitize()}_$ionType",
                serdeProperties)
    }

    fun testCases() = parseInput().map { listOf(it.hiveType, it.ionType, it.expected) }

    @Test
    @Parameters(method = "testCases")
    fun serializeAsTest(hiveType: String, ionType: String, expected: IonValue) {
        val tableName = tableName(hiveType, ionType)
        val serdeProperties = serdeProperties(ionType)
        createTable(tableName, hiveType, ionType, serdeProperties)

        val rawBytes = hive().queryToFileAndRead("SELECT * FROM $tableName", serdeProperties)
        val datagram = DOM_FACTORY.loader.load(rawBytes)

        assertEquals(1, datagram.size)
        val struct = datagram[0] as IonStruct

        assertEquals(1, struct.size())
        assertEquals(expected, struct.first())
    }
}
