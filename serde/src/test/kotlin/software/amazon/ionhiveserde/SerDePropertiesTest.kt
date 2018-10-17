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

@RunWith(JUnitParamsRunner::class)
class SerDePropertiesTest {

    private fun encodings() = IonEncoding.values()

    @Test
    @Parameters(method = "encodings")
    fun encoding(encoding: IonEncoding) {
        val subject = SerDeProperties(Properties().apply { setProperty("encoding", encoding.name) })

        assertEquals(encoding, subject.encoding)
    }

    @Test
    fun defaultEncoding() {
        val subject = SerDeProperties(Properties())

        assertEquals(IonEncoding.BINARY, subject.encoding)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidEncoding() {
        SerDeProperties(Properties().apply { setProperty("encoding", "not an encoding") })
    }

    @Test
    fun timestampOffsetInMinutes() {
        val subject = SerDeProperties(Properties().apply { setProperty("timestamp.serialization_offset", "01:00") })

        assertEquals(60, subject.timestampOffsetInMinutes)
    }

    @Test
    fun defaultTimestampOffsetInMinutes() {
        val subject = SerDeProperties(Properties())

        assertEquals(0, subject.timestampOffsetInMinutes)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidTimestampOffsetInMinutes() {
        SerDeProperties(Properties().apply { setProperty("timestamp.serialization_offset", "not an offset") })
    }
}