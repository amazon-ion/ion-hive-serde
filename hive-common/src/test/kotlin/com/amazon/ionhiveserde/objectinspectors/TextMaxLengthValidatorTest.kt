// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import org.junit.Test

class TextMaxLengthValidatorTest {
    private val subject = com.amazon.ionhiveserde.objectinspectors.TextMaxLengthValidator()

    @Test
    fun validateForValidString() {
        subject.validate("1234567", 7)
    }

    @Test(expected = IllegalArgumentException::class)
    fun validateForInvalidString() {
        subject.validate("1234567", 6)
    }

    @Test
    fun validateForSizeZero() {
        subject.validate("", 0)
    }

    @Test(expected = IllegalArgumentException::class)
    fun validateForInvalidSize() {
        subject.validate("1234567", -1)
    }
}
