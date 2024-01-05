/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.configuration

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class TimestampOffsetConfigTest {

    private fun makeConfig(offset: String) = com.amazon.ionhiveserde.configuration.TimestampOffsetConfig(
            MapBasedRawConfiguration(mapOf("ion.timestamp.serialization_offset" to offset)))

    private fun parseOffsetValidTestCases(): List<List<Any>> = listOf(
            listOf("Z", 0),
            listOf("00:00", 0),
            listOf("+00:00", 0),
            listOf("-00:00", 0),

            listOf("01:00", 60),
            listOf("+01:00", 60),
            listOf("-01:00", -60),

            listOf("00:01", 1),
            listOf("+00:01", 1),
            listOf("-00:01", -1),

            listOf("01:01", 61),
            listOf("+01:01", 61),
            listOf("-01:01", -61),

            listOf("23:59", 60 * 23 + 59),
            listOf("+23:59", 60 * 23 + 59),
            listOf("-23:59", -(60 * 23 + 59))
    )

    @Test
    @Parameters(method = "parseOffsetValidTestCases")
    fun timestampOffsetInMinutes(offsetText: String, expected: Int) {
        val subject = makeConfig(offsetText)

        assertEquals(expected, subject.timestampOffsetInMinutes)
    }

    @Test
    fun defaultTimestampOffsetInMinutes() {
        val subject = com.amazon.ionhiveserde.configuration.TimestampOffsetConfig(MapBasedRawConfiguration(mapOf()))

        assertEquals(0, subject.timestampOffsetInMinutes)
    }

    fun parseOffsetInvalidTestCases(): List<String> = listOf(
            "",
            "+Z",
            "1",
            "01",
            "001",
            "0001",
            "1000",
            "+10000",
            "+1:00",
            "+01:00:00",
            "two hours"
    )

    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "parseOffsetInvalidTestCases")
    fun invalidTimestampOffsetInMinutes(offsetText: String) {
        makeConfig(offsetText)
    }
}