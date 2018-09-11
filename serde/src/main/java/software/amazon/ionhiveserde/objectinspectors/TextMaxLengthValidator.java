/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ionhiveserde.objectinspectors;

/**
 * Validates a {@link String} length, throwing if the string is longer than expected.
 */
// FIXME: name
class TextMaxLengthValidator {

    /**
     * Validates and returns the string.
     *
     * @return the string s if it does not exceed the max length
     * @throws IllegalArgumentException if the string is longer than max length
     */
    String validate(final String s, final int maxLength) {
        if (s.length() > maxLength) {
            throw new IllegalArgumentException(
                "text is longer than allowed. Size: " + s.length() + ", max: " + maxLength);
        }

        return s;
    }
}
