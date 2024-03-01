// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonInt
import com.amazon.ionhiveserde.ION
import org.apache.hadoop.io.IntWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonIntToIntObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonInt, IntWritable, Int>() {

    override val subjectOverflow = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(false)
    override fun overflowTestCases() = listOf(
            OverflowTestCase(
                    ION.newInt(com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector.MAX_VALUE.toLong() + 1L),
                    com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector.MIN_VALUE,
                    IntWritable(com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector.MIN_VALUE)
            )
    )

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    override fun validTestCases() = listOf(ValidTestCase(ION.newInt(10), 10, IntWritable(10)))

    @Test
    fun get() {
        val testCase = validTestCases().first() // has only one
        assertEquals(testCase.expectedPrimitive, subject.get(testCase.ionValue))
    }


    @Test(expected = IllegalArgumentException::class)
    fun getOverflow() {
        val testCase = overflowTestCases().first() // has only one
        subject.get(testCase.ionValue)
    }

    @Test
    fun getOverflowWithoutFailOnOverflow() {
        val testCase = overflowTestCases().first() // has only one
        val actual = subjectOverflow.get(testCase.ionValue)
        assertEquals(testCase.expectedPrimitive, actual)
    }
}
