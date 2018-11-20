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

package software.amazon.ionhiveserde

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonType
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(JUnitParamsRunner::class)
class SerDePropertiesTest {

    // encoding ------------------------------------------------------------------------------------------------------

    private fun encodings() = IonEncoding.values()

    private fun makeSerDeProperties(properties: Properties = Properties(),
                                    columnNames: List<String> = listOf(),
                                    columnTypes: List<TypeInfo> = listOf()) =
            SerDeProperties(properties, columnNames, columnTypes)

    @Test
    @Parameters(method = "encodings")
    fun encoding(encoding: IonEncoding) {
        val subject = makeSerDeProperties(Properties().apply { setProperty("encoding", encoding.name) })

        assertEquals(encoding, subject.encoding)
    }

    @Test
    fun defaultEncoding() {
        val subject = makeSerDeProperties()

        assertEquals(IonEncoding.BINARY, subject.encoding)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidEncoding() {
        makeSerDeProperties(Properties().apply { setProperty("encoding", "not an encoding") })
    }

    // timestampOffsetInMinutes ---------------------------------------------------------------------------------------

    @Test
    fun timestampOffsetInMinutes() {
        val subject = makeSerDeProperties(
                Properties().apply { setProperty("timestamp.serialization_offset", "01:00") })

        assertEquals(60, subject.timestampOffsetInMinutes)
    }

    @Test
    fun defaultTimestampOffsetInMinutes() {
        val subject = makeSerDeProperties()

        assertEquals(0, subject.timestampOffsetInMinutes)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidTimestampOffsetInMinutes() {
        makeSerDeProperties(
                Properties().apply { setProperty("timestamp.serialization_offset", "not an offset") })
    }

    // serializeNull --------------------------------------------------------------------------------------------------

    private fun serializeNullOptions() = SerializeNullStrategy.values()

    @Test
    @Parameters(method = "serializeNullOptions")
    fun serializeNull(serializeNullStrategy: SerializeNullStrategy) {
        val subject = makeSerDeProperties(
                Properties().apply { setProperty("serialize_null", serializeNullStrategy.name) })

        assertEquals(serializeNullStrategy, subject.serializeNull)
    }

    @Test
    fun defaultSerializeNull() {
        val subject = makeSerDeProperties()

        assertEquals(SerializeNullStrategy.OMIT, subject.serializeNull)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidSerializeNull() {
        makeSerDeProperties(Properties().apply { setProperty("serialize_null", "invalid option") })
    }

    // failOnOverflow -------------------------------------------------------------------------------------------------

    @Test
    fun failOnOverflow() {
        val subject = makeSerDeProperties(
                Properties().apply {
                    setProperty("fail_on_overflow", "false") // sets default
                    setProperty("column_1.fail_on_overflow", "true")
                    setProperty("not_a_column.fail_on_overflow", "true")
                },
                listOf("column_1"),
                listOf(intTypeInfo))

        assertTrue(subject.failOnOverflowFor("column_1"))
        assertFalse(subject.failOnOverflowFor("not_a_column")) // not a column
        assertFalse(subject.failOnOverflowFor("not even in the config"))
    }

    @Test
    fun failOnOverflowDefault() {
        val subject = makeSerDeProperties(Properties(), listOf("column_1"), listOf(intTypeInfo))

        assertTrue(subject.failOnOverflowFor("column_1"))
        assertTrue(subject.failOnOverflowFor("not_a_column"))
    }

    @Test
    fun failOnOverflowNotBoolean() {
        val subject = makeSerDeProperties(
                Properties().apply { setProperty("fail_on_overflow", "not a boolean") },
                listOf("column_1"),
                listOf(intTypeInfo))

        assertFalse(subject.failOnOverflowFor("column_1"))
        assertFalse(subject.failOnOverflowFor("not_a_column")) // not a column
    }

    // serializationIonTypeFor ----------------------------------------------------------------------------------------

    private fun defaultSerializationMappings() = listOf<Any>(
            // fixed
            listOf(TypeInfoFactory.booleanTypeInfo, IonType.BOOL),
            listOf(TypeInfoFactory.byteTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.shortTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.intTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.longTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.floatTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.doubleTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.dateTypeInfo, IonType.TIMESTAMP),
            listOf(TypeInfoFactory.timestampTypeInfo, IonType.TIMESTAMP),

            // default for configurable
            listOf(TypeInfoFactory.stringTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.charTypeInfo,IonType.STRING),
            listOf(TypeInfoFactory.varcharTypeInfo,IonType.STRING),
            listOf(TypeInfoFactory.binaryTypeInfo,IonType.BLOB),
            listOf(TypeInfoFactory.decimalTypeInfo,IonType.DECIMAL)
    )

    @Test
    @Parameters(method = "defaultSerializationMappings")
    fun serializationIonTypeForDefaults(typeInfo: TypeInfo, expectedIonType: IonType) {
        val subject = makeSerDeProperties(Properties(), listOf("column_1"), listOf(typeInfo))

        assertEquals(expectedIonType, subject.serializationIonTypeFor(0))
    }

    private fun validSerializationMappings() = listOf<Any>(
            // fixed
            listOf(TypeInfoFactory.booleanTypeInfo, IonType.BOOL),
            listOf(TypeInfoFactory.byteTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.shortTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.intTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.longTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.floatTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.doubleTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.dateTypeInfo, IonType.TIMESTAMP),
            listOf(TypeInfoFactory.timestampTypeInfo, IonType.TIMESTAMP),

            // configurable
            listOf(TypeInfoFactory.stringTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.stringTypeInfo, IonType.SYMBOL),
            listOf(TypeInfoFactory.charTypeInfo,IonType.STRING),
            listOf(TypeInfoFactory.charTypeInfo,IonType.SYMBOL),
            listOf(TypeInfoFactory.varcharTypeInfo,IonType.STRING),
            listOf(TypeInfoFactory.varcharTypeInfo,IonType.SYMBOL),
            listOf(TypeInfoFactory.binaryTypeInfo,IonType.BLOB),
            listOf(TypeInfoFactory.binaryTypeInfo,IonType.CLOB),
            listOf(TypeInfoFactory.decimalTypeInfo,IonType.DECIMAL),
            listOf(TypeInfoFactory.decimalTypeInfo,IonType.INT)
    )

    @Test
    @Parameters(method = "validSerializationMappings")
    fun serializationIonType(typeInfo: TypeInfo, expectedIonType: IonType) {
        val subject = makeSerDeProperties(
                Properties().apply { setProperty("column.0.serialize_as", expectedIonType.name) },
                listOf("column_1"),
                listOf(typeInfo))

        assertEquals(expectedIonType, subject.serializationIonTypeFor(0))
    }

    private fun invalidSerializationMappings() = listOf<Any>(
            // fixed
            listOf(TypeInfoFactory.booleanTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.byteTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.shortTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.intTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.longTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.floatTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.doubleTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.dateTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.timestampTypeInfo, IonType.STRING),

            // configurable
            listOf(TypeInfoFactory.stringTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.charTypeInfo,IonType.FLOAT),
            listOf(TypeInfoFactory.varcharTypeInfo,IonType.FLOAT),
            listOf(TypeInfoFactory.binaryTypeInfo,IonType.FLOAT),
            listOf(TypeInfoFactory.decimalTypeInfo,IonType.FLOAT)
    )

    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "invalidSerializationMappings")
    fun serializationIonTypeInvalid(typeInfo: TypeInfo, expectedIonType: IonType) {
        makeSerDeProperties(
                Properties().apply { setProperty("column.0.serialize_as", expectedIonType.name) },
                listOf("column_1"),
                listOf(typeInfo)
        ).serializationIonTypeFor(0)
    }

    @Test(expected = IndexOutOfBoundsException::class)
    fun serializationIonTypeUnknownColumn() {
        makeSerDeProperties(
                Properties().apply { setProperty("column.0.serialize_as", IonType.INT.name) },
                listOf("column_1"),
                listOf(TypeInfoFactory.intTypeInfo)
        ).serializationIonTypeFor(1)
    }
}