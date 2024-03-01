// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonValue;

/**
 * Utility methods for Ion values.
 */
public class IonUtil {

    static boolean isIonNull(final IonValue ionValue) {
        return ionValue == null || ionValue.isNullValue();
    }

    public static <T extends IonValue> T handleNull(final T value) {
        return isIonNull(value) ? null : value;
    }
}
