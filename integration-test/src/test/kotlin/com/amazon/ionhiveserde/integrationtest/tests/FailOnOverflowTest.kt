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
class FailOnOverflowTest : com.amazon.ionhiveserde.integrationtest.Base() {
    companion object : com.amazon.ionhiveserde.integrationtest.TestLifecycle {
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
