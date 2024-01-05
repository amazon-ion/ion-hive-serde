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

import com.amazon.ion.IonInt
import com.amazon.ionhiveserde.ION
import junitparams.Parameters
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonIntToTinyIntObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonInt, ByteWritable, Byte>() {

    override val subjectOverflow = com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector(false)
    override fun overflowTestCases() = listOf(
            OverflowTestCase(
                    ION.newInt(com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector.MAX_VALUE + 1),
                    com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector.MIN_VALUE.toByte(),
                    ByteWritable(com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector.MIN_VALUE.toByte())
            )
    )

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector(true)
    override fun validTestCases() = listOf(
            com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector.MIN_VALUE,
            -10,
            0,
            10,
            com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector.MAX_VALUE)
            .map { ValidTestCase(ION.newInt(it), it.toByte(), ByteWritable(it.toByte())) }

    @Test
    @Parameters(method = "validTestCases")
    fun get(testCase: ValidTestCase<IonInt, ByteWritable, Byte>) {
        assertEquals(testCase.expectedPrimitive, subject.get(testCase.ionValue))
    }



    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "overflowTestCases")
    fun getOverflow(testCase: OverflowTestCase<IonInt, ByteWritable, Byte>) {
        subject.get(testCase.ionValue)
    }

    @Test
    @Parameters(method = "overflowTestCases")
    fun getOverflowWithoutFailOnOverflow(testCase: OverflowTestCase<IonInt, ByteWritable, Byte>) {
        val actual = subjectOverflow.get(testCase.ionValue)
        assertEquals(testCase.expectedPrimitive, actual)
    }
}