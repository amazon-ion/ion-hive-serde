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

package com.amazon.ionhiveserde

import com.amazon.ion.IonDatagram
import com.amazon.ion.IonSequence
import com.amazon.ion.IonStruct
import com.amazon.ion.system.IonSystemBuilder
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonSequenceCaseInsensitiveDecorator
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonStructCaseInsensitiveDecorator

internal val ION = IonSystemBuilder.standard().build()
internal val ionNull = ION.newNull()

internal fun datagram_for(s: String): IonDatagram {
    return ION.loader.load(s)
}

internal fun struct_for(s: String): IonStruct {
    val v = datagram_for(s).iterator().next()
    if (v !is IonStruct) throw IllegalArgumentException("Required an IonStruct, found ${v.javaClass.simpleName}")

    return v
}

internal fun case_insensitive_decorator_struct_for(s : String): IonStruct {
    return IonStructCaseInsensitiveDecorator(struct_for(s))
}

internal fun sequence_for(s: String): IonSequence {
    val v = datagram_for(s).iterator().next()
    if (v !is IonSequence) throw IllegalArgumentException("Required an IonSequence, found ${v.javaClass.simpleName}")

    return v
}

internal fun case_insensitive_decorator_sequence_for(s : String): IonSequence {
    return IonSequenceCaseInsensitiveDecorator(sequence_for(s))
}

/**
 * Create a sample struct used for testing. To create a desired Ion struct, use struct_for().
 */
internal fun makeStruct(): IonStruct {
    return struct_for("{a: 1, b: 2, c: 3}")
}

/**
 * Create a sample case insensitive struct used for testing. To create a desired case insensitive Ion struct,
 * use case_insensitive_decorator_struct_for().
 */
internal fun makeCaseInsensitiveStruct(): IonStruct {
    return case_insensitive_decorator_struct_for("{a: 1, b: null}")
}