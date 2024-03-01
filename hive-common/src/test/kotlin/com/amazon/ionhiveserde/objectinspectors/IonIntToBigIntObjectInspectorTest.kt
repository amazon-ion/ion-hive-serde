// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonInt
import com.amazon.ionhiveserde.ION
import junitparams.Parameters
import org.apache.hadoop.io.LongWritable
import org.junit.Test
import java.math.BigInteger
import kotlin.test.assertEquals

class IonIntToBigIntObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonInt, LongWritable, Long>() {

    override val subjectOverflow = com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector(false)
    override fun overflowTestCases() = listOf(
            OverflowTestCase(
                    ION.newInt(BigInteger.valueOf(com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector.MAX_VALUE).inc()),
                    com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector.MIN_VALUE,
                    LongWritable(com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector.MIN_VALUE)
            )
    )

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector(true)
    override fun validTestCases() = listOf(
            com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector.MIN_VALUE,
            10L,
            1L,
            com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector.MAX_VALUE)
            .map {ValidTestCase(ION.newInt(it), it, LongWritable(it))}

    @Test
    @Parameters(method = "validTestCases")
    fun get(testCase: ValidTestCase<IonInt, LongWritable, Long>) {
        assertEquals(testCase.expectedPrimitive, subject.get(testCase.ionValue))
    }


    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "overflowTestCases")
    fun getOverflow(testCase: OverflowTestCase<IonInt, LongWritable, Long>) {
        subject.get(testCase.ionValue)
    }

    @Test
    @Parameters(method = "overflowTestCases")
    fun getOverflowWithoutFailOnOverflow(testCase: OverflowTestCase<IonInt, LongWritable, Long>) {
        val actual = subjectOverflow.get(testCase.ionValue)
        assertEquals(testCase.expectedPrimitive, actual)
    }
}
