// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonFloat
import com.amazon.ionhiveserde.ION
import junitparams.Parameters
import org.apache.hadoop.io.DoubleWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonFloatToDoubleObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonFloat, DoubleWritable, Double>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonFloatToDoubleObjectInspector()

    override fun validTestCases() = listOf(
            ValidTestCase(ION.newFloat(1.123456789), 1.123456789, DoubleWritable(1.123456789)),
            ValidTestCase(ION.newFloat(1.0), 1.0, DoubleWritable(1.0))
    )

    @Test
    @Parameters(method = "validTestCases")
    fun get(testCase: ValidTestCase<IonFloat, DoubleWritable, Double>) {
        assertEquals(testCase.expectedPrimitive, subject.getPrimitiveJavaObject(testCase.ionValue))
    }
}
