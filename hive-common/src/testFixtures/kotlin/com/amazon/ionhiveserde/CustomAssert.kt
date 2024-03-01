// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde

import com.amazon.ion.IonValue
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonSequenceCaseInsensitiveDecorator
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonStructCaseInsensitiveDecorator
import java.lang.Exception
import kotlin.test.assertEquals

// Should be able to use standard library assertIs here after kotlin 1.5, figure this out.
fun assertInstanceOf(o: Any, type: Class<*>?) {
    assertEquals(o.javaClass, type)
}

fun assertStructWrapper(o: Any?) {
    assertInstanceOf(o!!, IonStructCaseInsensitiveDecorator::class.java)
}

fun assertSequenceWrapper(o: Any?) {
    assertInstanceOf(o!!, IonSequenceCaseInsensitiveDecorator::class.java)
}

fun assertMultiEquals(expected: Array<IonValue>, actual: Any) {
    for (exp in expected)
            try {
                assertEquals(exp, actual)
                return
            } catch (e: AssertionError) {
                continue
            }
    throw Exception("Can't find any expected value in the expected list.")
}
