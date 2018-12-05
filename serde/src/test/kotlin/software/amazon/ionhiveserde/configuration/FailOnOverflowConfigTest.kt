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

package software.amazon.ionhiveserde.configuration

import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class FailOnOverflowConfigTest {
    private fun makeConfig(map: Map<String, String>, columnNames: List<String>) =
            FailOnOverflowConfig(MapBasedRawConfiguration(map), columnNames)

    @Test
    fun failOnOverflow() {
        val subject = makeConfig(
                mapOf(
                        "ion.fail_on_overflow" to "false", // sets default
                        "ion.column_1.fail_on_overflow" to "true",
                        "ion.not_a_column.fail_on_overflow" to "true"
                ),
                listOf("column_1"))

        assertTrue(subject.failOnOverflowFor("column_1"))
        assertFalse(subject.failOnOverflowFor("not_a_column")) // not a column
        assertFalse(subject.failOnOverflowFor("not even in the config"))
    }

    @Test
    fun failOnOverflowDefault() {
        val subject = makeConfig(mapOf(), listOf("column_1"))

        assertTrue(subject.failOnOverflowFor("column_1"))
        assertTrue(subject.failOnOverflowFor("not_a_column"))
    }

    @Test
    fun failOnOverflowNotBoolean() {
        val subject = makeConfig(mapOf("ion.fail_on_overflow" to "not a boolean"), listOf("column_1"))

        assertFalse(subject.failOnOverflowFor("column_1"))
        assertFalse(subject.failOnOverflowFor("not_a_column")) // not a column
    }
}