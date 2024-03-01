// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonFloat
import com.amazon.ionhiveserde.ION
import junitparams.Parameters
import org.apache.hadoop.io.FloatWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonFloatToFloatObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonFloat, FloatWritable, Float>() {


    override val subject = com.amazon.ionhiveserde.objectinspectors.IonFloatToFloatObjectInspector(true)
    override fun validTestCases() = listOf(10.0, 1.0)
            .map { ValidTestCase(ION.newFloat(it), it.toFloat(), FloatWritable(it.toFloat())) }

    override val subjectOverflow = com.amazon.ionhiveserde.objectinspectors.IonFloatToFloatObjectInspector(false)
    override fun overflowTestCases() = listOf(
            OverflowTestCase(ION.newFloat(1.123456789), 1.1234568.toFloat(), FloatWritable(1.1234568.toFloat())),
            OverflowTestCase(ION.newFloat(1.999999999), 2.0.toFloat(), FloatWritable(2.0.toFloat()))
    )

    @Test
    @Parameters(method = "validTestCases")
    fun get(testCase: ValidTestCase<IonFloat, FloatWritable, Float>) {
        assertEquals(testCase.expectedPrimitive, subject.getPrimitiveJavaObject(testCase.ionValue))
    }

    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "overflowTestCases")
    fun getOverflow(testCase: OverflowTestCase<IonFloat, FloatWritable, Float>) {
        subject.get(testCase.ionValue)
    }

    @Test
    @Parameters(method = "overflowTestCases")
    fun getOverflowWithoutFailOnOverflow(testCase: OverflowTestCase<IonFloat, FloatWritable, Float>) {
        assertEquals(testCase.expectedPrimitive, subjectOverflow.getPrimitiveJavaObject(testCase.ionValue))
    }
}

