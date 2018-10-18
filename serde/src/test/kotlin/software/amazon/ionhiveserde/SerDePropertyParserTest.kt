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
import software.amazon.ionhiveserde.util.SerDePropertyParser.parseOffset
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class SerDePropertyParserTest {

    // parseOffset ----------------------------------------------------------------------------------------------------

    fun parseOffsetValidTestCases(): List<List<Any>> = listOf(
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

            listOf("23:59", 60*23+59),
            listOf("+23:59", 60*23+59),
            listOf("-23:59", -(60*23+59))
    )

    @Test
    @Parameters(method = "parseOffsetValidTestCases")
    fun parseValidOffset(offsetText: String, expected: Int) {
        assertEquals(expected, parseOffset(offsetText))
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
    fun parseInvalidOffset(offsetText: String) {
         parseOffset(offsetText)
    }

    @Test(expected = IllegalArgumentException::class)
    fun parseNullOffset() {
        parseOffset(null)
    }
}