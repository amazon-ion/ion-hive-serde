// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonValue
import junitparams.Parameters
import org.apache.hadoop.io.Writable
import org.junit.Test
import kotlin.test.assertEquals

/**
 * Base class for primitive ObjectInspector that can overflow. Extends [AbstractIonPrimitiveJavaObjectInspectorTest].
 *
 * Included tests using data from `overflowTestCases`.
 * - getPrimitiveWritableObject failing on overflow.
 * - getPrimitiveJavaObject failing on overflow.
 * - getPrimitiveWritableObject overflowing.
 * - getPrimitiveJavaObject overflowing.
 */
abstract class AbstractOverflowablePrimitiveObjectInspectorTest<I : IonValue, W : Writable, P>
    : AbstractIonPrimitiveJavaObjectInspectorTest<I, W, P>() {

    data class OverflowTestCase<out I : IonValue, W : Writable, P>(val ionValue: I,
                                                               val expectedPrimitive: P,
                                                               val expectedWritable: W)

    /**
     * Object inspector configured to overflow instead of failing.
     */
    protected abstract val subjectOverflow: com.amazon.ionhiveserde.objectinspectors.AbstractIonPrimitiveJavaObjectInspector

    /**
     * Test cases that cause an overflow.
     */
    abstract fun overflowTestCases(): List<OverflowTestCase<I, W, P>>

    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "overflowTestCases")
    open fun getPrimitiveWritableObjectOverflow(testCase: OverflowTestCase<I, W, P>) {
        subject.getPrimitiveWritableObject(testCase.ionValue)
    }

    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "overflowTestCases")
    open fun getPrimitiveJavaObjectOverflow(testCase: OverflowTestCase<I, W, P>) {
        subject.getPrimitiveJavaObject(testCase.ionValue)
    }

    @Test
    @Parameters(method = "overflowTestCases")
    open fun getPrimitiveWritableObjectOverflowWithoutFailOnOverflow(testCase: OverflowTestCase<I, W, P>) {
        assertEquals(testCase.expectedWritable, subjectOverflow.getPrimitiveWritableObject(testCase.ionValue))
    }

    @Test
    @Parameters(method = "overflowTestCases")
    open fun getPrimitiveJavaObjectOverflowWithoutFailOnOverflow(testCase: OverflowTestCase<I, W, P>) {
        assertEquals(testCase.expectedPrimitive, subjectOverflow.getPrimitiveJavaObject(testCase.ionValue))
    }
}
