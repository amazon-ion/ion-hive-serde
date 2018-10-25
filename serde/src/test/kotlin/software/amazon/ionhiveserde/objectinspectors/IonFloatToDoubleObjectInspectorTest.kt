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
import org.apache.hadoop.io.DoubleWritable
import org.junit.Test
import software.amazon.ion.IonFloat
import software.amazon.ionhiveserde.ION
import kotlin.test.assertEquals

class IonFloatToDoubleObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonFloat, DoubleWritable, Double>() {

    override val subject = IonFloatToDoubleObjectInspector()

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