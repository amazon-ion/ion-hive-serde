/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors

import org.junit.Test
import kotlin.test.assertEquals

class TextMaxLengthValidatorTest {
    private val subject = TextMaxLengthValidator()

    @Test
    fun validateForValidString() {
        val expected = "1234567"
        val actual = subject.validate(expected, 7)

        assertEquals(expected, actual)
    }

    @Test(expected = IllegalArgumentException::class)
    fun validateForInvalidString() {
        subject.validate("1234567", 6)
    }

    @Test
    fun validateForSizeZero() {
        val expected = ""
        val actual = subject.validate(expected, 0)

        assertEquals(expected, actual)
    }

    @Test(expected = IllegalArgumentException::class)
    fun validateForInvalidSize() {
        subject.validate("1234567", -1)
    }
}