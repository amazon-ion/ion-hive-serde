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

package com.amazon.ionhiveserde.caseinsensitivedecorator;

import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;

public class IonCaseInsensitiveDecorator {
    /**
     * Wraps an IonValue in an IonContainerCaseInsensitiveDecorator if it's an ion container.

     * @return a case insensitive decorator wrapped Ion Value.
     */
    public static IonValue wrapValue(final IonValue v) {
        if (v == null || v.isNullValue()) {
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
