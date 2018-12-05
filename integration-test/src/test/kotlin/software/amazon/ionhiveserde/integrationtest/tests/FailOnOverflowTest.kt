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
class FailOnOverflowTest : Base() {
    companion object : TestLifecycle {
        private const val TABLE_NAME = "FailOnOverflowTest"
        private const val TEST_DIR = "$SHARED_DIR/input/$TABLE_NAME/"
        private const val INPUT = """
            { hiveType: "TINYINT",              value: 128,          expected: -128 }
            { hiveType: "VARCHAR(5)",           value: "1234567890", expected: "12345" }
            { hiveType: "STRUCT<foo: TINYINT>", value: {foo: 128},   expected: {foo: -128} }
        """

        private val serdeProperties = mapOf("ion.fail_on_overflow" to "false")

        data class TestCase(val hiveType: String, val value: IonValue, val expected: IonValue)

        private fun parseInput() = DOM_FACTORY.loader.load(INPUT)
                .map { it as IonStruct }
                .map {
                    TestCase(
                            (it["hiveType"] as IonText).stringValue(),
                            it["value"],
                            it["expected"])
                }

        override fun setup() {
            mkdir(TEST_DIR)

            parseInput().forEach { testCase ->
                val path = "$TEST_DIR/${testCase.hiveType.sanitize()}"
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

    private fun createTable(hiveType: String) {
        hive().createExternalTable(
                TABLE_NAME,
                mapOf("field" to hiveType),
                "/data/input/$TABLE_NAME/${hiveType.sanitize()}",
                serdeProperties)
    }

    fun testCases() = parseInput().map { listOf(it.hiveType, it.expected) }

    @Test
    @Parameters(method = "testCases")
    fun failOnOverflowTest(hiveType: String, expected: IonValue) {
        createTable(hiveType)

        val rawBytes = hive().queryToFileAndRead("SELECT field FROM $TABLE_NAME", serdeProperties)
        val datagram = DOM_FACTORY.loader.load(rawBytes)

        assertEquals(1, datagram.size)
        val struct = datagram[0] as IonStruct

        assertEquals(1, struct.size())
        assertEquals(expected, struct.first())
    }
}
