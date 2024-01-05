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

