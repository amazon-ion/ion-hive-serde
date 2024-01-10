// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

/**
 * Validates a {@link String} length, throwing if the string is longer than expected.
 */
class TextMaxLengthValidator {

    /**
     * Validates text size.
     *
     * @throws IllegalArgumentException if the string is longer than max length
     */
    void validate(final String text, final int maxLength) {
        if (text.length() > maxLength) {
            throw new IllegalArgumentException(
                "text is longer than allowed. Size: " + text.length() + ", max: " + maxLength);
        }
    }
}
