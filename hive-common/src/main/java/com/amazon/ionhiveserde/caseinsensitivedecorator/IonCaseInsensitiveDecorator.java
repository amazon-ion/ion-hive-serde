// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.caseinsensitivedecorator;

import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;

public class IonCaseInsensitiveDecorator {
    /**
     * Wraps an IonValue in an IonContainerCaseInsensitiveDecorator if it's an ion container.
     *
     * @return a case insensitive decorator wrapped Ion Value.
     */
    public static IonValue wrapValue(final IonValue v) {
        if (v == null) {
            return null;
        }

        switch (v.getType()) {
            case LIST:
                // fallthrough
            case SEXP:
                return new IonSequenceCaseInsensitiveDecorator((IonSequence) v);
            case STRUCT:
                return new IonStructCaseInsensitiveDecorator((IonStruct) v);
            default:
                return v;
        }
    }
}
