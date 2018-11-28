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
import junitparams.Parameters
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonStruct
import software.amazon.ion.IonText
import software.amazon.ion.IonType
import software.amazon.ion.IonValue
import software.amazon.ionhiveserde.integrationtest.*
import software.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class SerializeAsTest : Base() {
    companion object : TestLifecycle {
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

        private fun parseInput() = ION.loader.load(INPUT)
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

                ION.newBinaryWriterFromPath("$path/file.10n").use { writer ->
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

    private fun serdeProperties(ionType: String) = mapOf("column.0.serialize_as" to ionType)
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
        val datagram = ION.loader.load(rawBytes)

        assertEquals(1, datagram.size)
        val struct = datagram[0] as IonStruct

        assertEquals(1, struct.size())
        assertEquals(expected, struct.first())
    }
}
