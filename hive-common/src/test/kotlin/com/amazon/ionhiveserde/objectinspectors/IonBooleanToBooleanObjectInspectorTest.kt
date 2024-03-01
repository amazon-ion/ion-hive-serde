// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonBool
import com.amazon.ionhiveserde.ION
import junitparams.Parameters
import org.apache.hadoop.io.BooleanWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonBooleanToBooleanObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonBool, BooleanWritable, Boolean>() {

    override fun validTestCases() = listOf(
            true,
            false)
            .map { ValidTestCase(ION.newBool(it), it, BooleanWritable(it)) }


    override val subject = com.amazon.ionhiveserde.objectinspectors.IonBooleanToBooleanObjectInspector()

    @Test
    @Parameters(method = "validTestCases")
    fun get(testCase: ValidTestCase<IonBool, BooleanWritable, Boolean>) {
        assertEquals(testCase.expectedPrimitive, subject.get(testCase.ionValue))
    }
}
