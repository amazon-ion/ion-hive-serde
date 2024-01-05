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
