// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde

import com.amazon.ion.IonDatagram
import com.amazon.ion.IonSequence
import com.amazon.ion.IonStruct
import com.amazon.ion.IonValue
import com.amazon.ion.system.IonSystemBuilder
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonCaseInsensitiveDecorator

val ION = IonSystemBuilder.standard().build()
val ionNull = ION.newNull()

fun datagram_for(s: String): IonDatagram {
    return ION.loader.load(s)
}

fun struct_for(s: String): IonStruct {
    val v = datagram_for(s).iterator().next()
    if (v !is IonStruct) throw IllegalArgumentException("Required an IonStruct, found ${v.javaClass.simpleName}")

    return v
}

fun sequence_for(s: String): IonSequence {
    val v = datagram_for(s).iterator().next()
    if (v !is IonSequence) throw IllegalArgumentException("Required an IonSequence, found ${v.javaClass.simpleName}")

    return v
}

fun case_insensitive(v: IonValue): IonValue {
    return IonCaseInsensitiveDecorator.wrapValue(v)
}
