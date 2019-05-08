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