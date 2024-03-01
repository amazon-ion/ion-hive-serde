// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonInt
import com.amazon.ionhiveserde.ION
import junitparams.Parameters
import org.apache.hadoop.io.ShortWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonIntToSmallIntObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonInt, ShortWritable, Short>() {

    override val subjectOverflow = com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector(false)
    override fun overflowTestCases() = listOf(
            OverflowTestCase(
                    ION.newInt(com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector.MAX_VALUE + 1),
                    com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector.MIN_VALUE.toShort(),
                    ShortWritable(com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector.MIN_VALUE.toShort())
            )
    )

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector(true)
    override fun validTestCases() = listOf(
            com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector.MIN_VALUE,
            -10,
            0,
            10,
            com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector.MAX_VALUE)
            .map { ValidTestCase(ION.newInt(it), it.toShort(), ShortWritable(it.toShort())) }

    @Test
    @Parameters(method = "validTestCases")
    fun get(testCase: ValidTestCase<IonInt, ShortWritable, Short>) {
        assertEquals(testCase.expectedPrimitive, subject.get(testCase.ionValue))
    }


    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "overflowTestCases")
    fun getOverflow(testCase: OverflowTestCase<IonInt, ShortWritable, Short>) {
        subject.get(testCase.ionValue)
    }

    @Test
    @Parameters(method = "overflowTestCases")
    fun getOverflowWithoutFailOnOverflow(testCase: OverflowTestCase<IonInt, ShortWritable, Short>) {
        val actual = subjectOverflow.get(testCase.ionValue)
        assertEquals(testCase.expectedPrimitive, actual)
    }
}
