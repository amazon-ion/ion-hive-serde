/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors

import junitparams.Parameters
import org.apache.hadoop.io.Writable
import org.junit.Test
import software.amazon.ion.IonValue
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
    protected abstract val subjectOverflow: AbstractIonPrimitiveJavaObjectInspector

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