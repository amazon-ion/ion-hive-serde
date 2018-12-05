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

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class EncodingConfigTest {
    private fun encodings() = IonEncoding.values()

    private fun makeConfig(encoding: String) = EncodingConfig(
            MapBasedRawConfiguration(mapOf("ion.encoding" to encoding)))

    @Test
    @Parameters(method = "encodings")
    fun encoding(encoding: IonEncoding) {
        val subject = makeConfig(encoding.name)

        assertEquals(encoding, subject.encoding)
    }

    @Test
    fun defaultEncoding() {
        val subject = EncodingConfig(MapBasedRawConfiguration(mapOf()))

        assertEquals(IonEncoding.BINARY, subject.encoding)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidEncoding() {
        makeConfig("not an encoding")
    }

}