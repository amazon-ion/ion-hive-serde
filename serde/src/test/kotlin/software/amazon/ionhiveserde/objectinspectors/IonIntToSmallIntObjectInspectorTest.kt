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

package software.amazon.ionhiveserde.objectinspectors

import junitparams.Parameters
import org.apache.hadoop.io.ShortWritable
import org.junit.Test
import software.amazon.ion.IonInt
import software.amazon.ionhiveserde.ION
import kotlin.test.assertEquals

class IonIntToSmallIntObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonInt, ShortWritable, Short>() {

    override val subjectOverflow = IonIntToSmallIntObjectInspector(false)
    override fun overflowTestCases() = listOf(
            OverflowTestCase(
                    ION.newInt(IonIntToSmallIntObjectInspector.MAX_VALUE + 1),
                    IonIntToSmallIntObjectInspector.MIN_VALUE.toShort(),
                    ShortWritable(IonIntToSmallIntObjectInspector.MIN_VALUE.toShort())
            )
    )

    override val subject = IonIntToSmallIntObjectInspector(true)
    override fun validTestCases() = listOf(
            IonIntToSmallIntObjectInspector.MIN_VALUE,
            -10,
            0,
            10,
            IonIntToSmallIntObjectInspector.MAX_VALUE)
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