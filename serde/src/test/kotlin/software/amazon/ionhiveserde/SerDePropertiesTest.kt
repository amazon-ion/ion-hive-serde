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
import org.junit.Test
import org.junit.runner.RunWith
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(JUnitParamsRunner::class)
class SerDePropertiesTest {

    // encoding ------------------------------------------------------------------------------------------------------

    private fun encodings() = IonEncoding.values()

    @Test
    @Parameters(method = "encodings")
    fun encoding(encoding: IonEncoding) {
        val subject = SerDeProperties(Properties().apply { setProperty("encoding", encoding.name) }, listOf())

        assertEquals(encoding, subject.encoding)
    }

    @Test
    fun defaultEncoding() {
        val subject = SerDeProperties(Properties(), listOf())

        assertEquals(IonEncoding.BINARY, subject.encoding)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidEncoding() {
        SerDeProperties(Properties().apply { setProperty("encoding", "not an encoding") }, listOf())
    }

    // timestampOffsetInMinutes ---------------------------------------------------------------------------------------

    @Test
    fun timestampOffsetInMinutes() {
        val subject = SerDeProperties(
                Properties().apply { setProperty("timestamp.serialization_offset", "01:00") },
                listOf())

        assertEquals(60, subject.timestampOffsetInMinutes)
    }

    @Test
    fun defaultTimestampOffsetInMinutes() {
        val subject = SerDeProperties(Properties(), listOf())

        assertEquals(0, subject.timestampOffsetInMinutes)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidTimestampOffsetInMinutes() {
        SerDeProperties(
                Properties().apply { setProperty("timestamp.serialization_offset", "not an offset") },
                listOf())
    }

    // serializeNull --------------------------------------------------------------------------------------------------

    private fun serializeNullOptions() = SerializeNullStrategy.values()

    @Test
    @Parameters(method = "serializeNullOptions")
    fun serializeNull(serializeNullStrategy: SerializeNullStrategy) {
        val subject = SerDeProperties(
                Properties().apply { setProperty("serialize_null", serializeNullStrategy.name) },
                listOf())

        assertEquals(serializeNullStrategy, subject.serializeNull)
    }

    @Test
    fun defaultSerializeNull() {
        val subject = SerDeProperties(Properties(), listOf())

        assertEquals(SerializeNullStrategy.OMIT, subject.serializeNull)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidSerializeNull() {
        SerDeProperties(Properties().apply { setProperty("serialize_null", "invalid option") }, listOf())
    }

    // failOnOverflow -------------------------------------------------------------------------------------------------

    @Test
    fun failOnOverflow() {
        val subject = SerDeProperties(
                Properties().apply {
                    setProperty("fail_on_overflow", "false") // sets default
                    setProperty("column_1.fail_on_overflow", "true")
                    setProperty("not_a_column.fail_on_overflow", "true")
                },
                listOf("column_1"))

        assertTrue(subject.failOnOverflowFor("column_1"))
        assertFalse(subject.failOnOverflowFor("not_a_column")) // not a column
        assertFalse(subject.failOnOverflowFor("not even in the config"))
    }

    @Test
    fun failOnOverflowDefault() {
        val subject = SerDeProperties(Properties(), listOf("column_1"))

        assertTrue(subject.failOnOverflowFor("column_1"))
        assertTrue(subject.failOnOverflowFor("not_a_column"))
    }

    @Test
    fun failOnOverflowNotBoolean() {
        val subject = SerDeProperties(
                Properties().apply { setProperty("fail_on_overflow", "not a boolean") },
                listOf("column_1"))

        assertFalse(subject.failOnOverflowFor("column_1"))
        assertFalse(subject.failOnOverflowFor("not_a_column")) // not a column
    }
}