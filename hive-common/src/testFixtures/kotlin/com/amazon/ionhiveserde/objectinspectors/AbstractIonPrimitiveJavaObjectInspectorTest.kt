// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonValue
import com.amazon.ionhiveserde.ionNull
import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.apache.hadoop.io.Writable
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Base class for primitive ObjectInspector test cases.
 *
 * Included tests
 * - null values
 * - getPrimitiveWritableObject using validTestCases values
 * - getPrimitiveJavaObject using validTestCases values
 */
@RunWith(JUnitParamsRunner::class)
abstract class AbstractIonPrimitiveJavaObjectInspectorTest<I : IonValue, W : Writable, P> {

    data class ValidTestCase<I : IonValue, W : Writable, P>(val ionValue: I,
                                                            val expectedPrimitive: P,
                                                            val expectedWritable: W)

    protected abstract val subject: com.amazon.ionhiveserde.objectinspectors.AbstractIonPrimitiveJavaObjectInspector

    abstract fun validTestCases(): List<ValidTestCase<out I, W, P>>

    @Test
    @Parameters(method = "validTestCases")
    open fun getPrimitiveWritableObject(testCase: ValidTestCase<out I, W, P>) =
            assertEquals(testCase.expectedWritable, subject.getPrimitiveWritableObject(testCase.ionValue))

    @Test
    @Parameters(method = "validTestCases")
    open fun getPrimitiveJavaObject(testCase: ValidTestCase<out I, W, P>) =
            assertEquals(testCase.expectedPrimitive, subject.getPrimitiveJavaObject(testCase.ionValue))

    @Test
    open fun getPrimitiveWritableObjectForNull() {
        assertNull(subject.getPrimitiveWritableObject(null))
    }

    @Test
    open fun getPrimitiveWritableObjectForIonNull() {
        assertNull(subject.getPrimitiveWritableObject(ionNull))
    }

    @Test
    open fun getPrimitiveJavaObjectForNull() {
        assertNull(subject.getPrimitiveJavaObject(null))
    }

    @Test
    open fun getPrimitiveJavaObjectForIonNull() {
        assertNull(subject.getPrimitiveJavaObject(ionNull))
    }
}

