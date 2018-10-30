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
import org.apache.hadoop.io.BooleanWritable
import org.junit.Test
import software.amazon.ion.IonBool
import software.amazon.ionhiveserde.ION
import kotlin.test.assertEquals

class IonBooleanToBooleanObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonBool, BooleanWritable, Boolean>() {

    override fun validTestCases() = listOf(
            true,
            false)
            .map { ValidTestCase(ION.newBool(it), it, BooleanWritable(it)) }


    override val subject = IonBooleanToBooleanObjectInspector()

    @Test
    @Parameters(method = "validTestCases")
    fun get(testCase: ValidTestCase<IonBool, BooleanWritable, Boolean>) {
        assertEquals(testCase.expectedPrimitive, subject.get(testCase.ionValue))
    }
}