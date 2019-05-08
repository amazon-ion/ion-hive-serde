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

import com.amazon.ion.*
import com.amazon.ionhiveserde.integrationtest.*
import com.amazon.ionhiveserde.integrationtest.docker.SHARED_DIR
import com.amazon.ionhiveserde.integrationtest.setup.TestData
import junitparams.JUnitParamsRunner
import junitparams.Parameters
import junitparams.naming.TestCaseName
import org.junit.Test
import org.junit.runner.RunWith
import java.sql.ResultSet
import java.sql.Types
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Test mapping between Ion and Hive types.
 */
@RunWith(JUnitParamsRunner::class)
class TypeMappingTest : com.amazon.ionhiveserde.integrationtest.Base() {

    /** Called by [Lifecycle] */
    companion object : com.amazon.ionhiveserde.integrationtest.TestLifecycle {
        private const val TYPE_MAPPING_DIR = "$SHARED_DIR/input/type-mapping"

        override fun setup() {
            mkdir(TYPE_MAPPING_DIR)

            TestData.typeMapping.forEach { value ->
                val hiveType = value.fieldName.sanitize()
                val values = value as IonSequence

                val path = "$TYPE_MAPPING_DIR/$hiveType"
                mkdir(path)

                val writers = sequenceOf(
                        newTextWriterFromPath("$path/$hiveType.ion"),
                        newBinaryWriterFromPath("$path/$hiveType.10n")
                )

                writers.forEach { writer -> writer.use { writeTestFile(it, values) } }
            }
        }

        override fun tearDown() {
            rm(TYPE_MAPPING_DIR)
        }

        private fun writeTestFile(writer: IonWriter, values: IonSequence) {
            values.forEach { ionValue ->
                writer.stepIn(IonType.STRUCT)
                writer.setFieldName("field")
                ionValue.writeTo(writer)
                writer.stepOut()
            }
        }
    }

    private fun hdfsPath(hiveType: String): String = "/data/input/type-mapping/${hiveType.sanitize()}"

    data class TestCase(val hiveType: String, val hdfsPath: String, val expectedIonValues: List<IonValue>)

    private val lossyTypes = arrayOf("TIMESTAMP", "DATE")
    private val testCases = TestData.typeMapping.asSequence()
            .map { it as IonSequence }
            .map {
                val expectedValues = it + it // same values are stored as text and binary so we double
                TestCase(it.fieldName, hdfsPath(it.fieldName), expectedValues)
            }

    private fun createTable(tableName: String, testCase: TestCase, serdeProperties: Map<String, String> = emptyMap()) {
        hive().createExternalTable(tableName, mapOf("field" to testCase.hiveType), testCase.hdfsPath, serdeProperties)
    }

    private fun testCaseFor(hiveType: String) = testCases.find { it.hiveType == hiveType }!!
    private val timestampTestCase by lazy { testCaseFor("TIMESTAMP") }
    private val dateTestCase by lazy { testCaseFor("DATE") }

    private fun jdbcTestTemplate(tableName: String,
                                 testCase: TestCase,
                                 assertions: (expected: IonValue, rs: ResultSet) -> Unit) {
        createTable(tableName, testCase)

        hive().query("SELECT * FROM $tableName") { rs ->

            testCase.expectedIonValues.forEach { expected ->
                assertTrue(rs.next())
                assertions(expected, rs)
            }

            assertFalse(rs.next())
        }
    }

    private fun ResultSet.getIon(index: Int) = when (this.metaData.getColumnType(index)) {
        Types.BOOLEAN -> DOM_FACTORY.newBool(this.getBoolean(index))
        Types.TINYINT, Types.INTEGER -> DOM_FACTORY.newInt(this.getInt(index))
        Types.BIGINT -> DOM_FACTORY.newInt(this.getLong(index))
        Types.FLOAT, Types.DOUBLE -> DOM_FACTORY.newFloat(this.getDouble(index))
        Types.DECIMAL -> DOM_FACTORY.newDecimal(this.getBigDecimal(index).stripTrailingZeros())
        Types.BINARY -> DOM_FACTORY.newBlob(this.getBinaryStream(index).readBytes())
        Types.CHAR, Types.VARCHAR -> DOM_FACTORY.newString(this.getString(index).trim())

        // java object is used for maps
        Types.ARRAY, Types.JAVA_OBJECT, Types.STRUCT -> {
            // hive jdbc driver represents this types a json string
            DOM_FACTORY.singleValue(this.getString(index))
        }
        else -> {
            throw IllegalArgumentException("Unknown sql column type: ${this.metaData.getColumnType(index)}")
        }
    }

    fun losslessTypes(): List<List<Any>> = testCases
            .filterNot { lossyTypes.contains(it.hiveType) }
            .map { listOf(it.hiveType, it) }
            .toList()

    @Test
    @Parameters(method = "losslessTypes")
    @TestCaseName("[{index}] {method}: {0}")
    fun losslessTypesJdbc(hiveType: String, testCase: TestCase) {
        jdbcTestTemplate("jdbcTypeMapping_${hiveType.sanitize()}", testCase) { expected, rs ->
            assertEquals(expected, rs.getIon(1))
        }
    }

    @Test
    fun timestampJdbc() {
        jdbcTestTemplate("jdbcTypeMapping_timestamp", timestampTestCase) { expected, rs ->
            val expectedTimestamp = expected as IonTimestamp
            val actual = rs.getTimestamp(1).toLocalDateTime().atOffset(ZoneOffset.UTC)

            assertEquals(
                    expectedTimestamp.millis,
                    actual.toInstant().toEpochMilli(),
                    message = """|Expected: $expectedTimestamp
                                 |Actual:   $actual
                                 |""".trimMargin())
        }
    }

    @Test
    fun dateJdbc() {
        jdbcTestTemplate("jdbcTypeMapping_date", dateTestCase) { expected, rs ->
            val expectedTimestamp = (expected as IonTimestamp).timestampValue()
            val actualDate = rs.getDate(1).toLocalDate()

            val assertErrorMessage = """|Expected: $expectedTimestamp
                                        |Actual:   $actualDate
                                        |""".trimMargin()

            assertEquals(expectedTimestamp.year, actualDate.year, message = assertErrorMessage)
            assertEquals(expectedTimestamp.month, actualDate.monthValue, message = assertErrorMessage)
            assertEquals(expectedTimestamp.day, actualDate.dayOfMonth, message = assertErrorMessage)
        }
    }

    private fun fileTestTemplate(tableName: String,
                                 testCase: TestCase,
                                 encoding: com.amazon.ionhiveserde.configuration.IonEncoding,
                                 assertions: (expected: IonValue, actual: IonValue) -> Unit) {
        createTable(tableName, testCase, mapOf("ion.encoding" to encoding.name))

        val rawBytes = hive().queryToFileAndRead("SELECT * FROM $tableName")
        val datagram = DOM_FACTORY.loader.load(rawBytes)

        assertEquals(testCase.expectedIonValues.size, datagram.size)
        testCase.expectedIonValues.forEachIndexed { index, expected ->
            val actualStruct = datagram[index] as IonStruct
            assertions(expected, actualStruct.first())
        }
    }

    fun losslessTypesAndEncoding(): List<List<Any>> = com.amazon.ionhiveserde.configuration.IonEncoding.values().flatMap { encoding ->
        losslessTypes().map { it + listOf(encoding) }
    }

    @Test
    @Parameters(method = "losslessTypesAndEncoding")
    @TestCaseName("[{index}] {method}: {0} - {2}")
    fun losslessTypesToFile(hiveType: String, testCase: TestCase, encoding: com.amazon.ionhiveserde.configuration.IonEncoding) {
        val tableName = "jdbcTypeMapping_${hiveType.sanitize()}"
        fileTestTemplate(tableName, testCase, encoding) { expected, actual -> assertEquals(expected, actual) }
    }

    fun encodings() = com.amazon.ionhiveserde.configuration.IonEncoding.values()

    @Test
    @Parameters(method = "encodings")
    @TestCaseName("[{index}] {method}: {0}")
    fun timestampToFile(encoding: com.amazon.ionhiveserde.configuration.IonEncoding) {
        fileTestTemplate("toFileTypeMapping_timestamp", timestampTestCase, encoding) { expected, actual ->
            val expectedTimestamp = (expected as IonTimestamp).timestampValue()
            val actualTimestamp = (actual as IonTimestamp).timestampValue()

            assertEquals(
                    expectedTimestamp.millis,
                    actualTimestamp.millis,
                    message = """|Expected: $expectedTimestamp
                                 |Actual:   $actualTimestamp
                                 |""".trimMargin())
        }
    }

    @Test
    @Parameters(method = "encodings")
    @TestCaseName("[{index}] {method}: {0}")
    fun dateToFile(encoding: com.amazon.ionhiveserde.configuration.IonEncoding) {
        fileTestTemplate("toFileTypeMapping_date", dateTestCase, encoding) { expected, actual ->
            val expectedTimestamp = (expected as IonTimestamp).timestampValue()
            val actualTimestamp = (actual as IonTimestamp).timestampValue()

            val assertErrorMessage = """|Expected: $expectedTimestamp
                                        |Actual:   $actualTimestamp
                                        |""".trimMargin()

            assertEquals(expectedTimestamp.year, actualTimestamp.year, message = assertErrorMessage)
            assertEquals(expectedTimestamp.month, actualTimestamp.month, message = assertErrorMessage)
            assertEquals(expectedTimestamp.day, actualTimestamp.day, message = assertErrorMessage)
            assertEquals(0, actualTimestamp.hour, message = assertErrorMessage)
            assertEquals(0, actualTimestamp.minute, message = assertErrorMessage)
            assertEquals(0, actualTimestamp.second, message = assertErrorMessage)
        }
    }
}
