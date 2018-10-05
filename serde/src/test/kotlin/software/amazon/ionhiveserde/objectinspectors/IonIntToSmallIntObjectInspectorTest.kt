/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors

import org.apache.hadoop.io.ShortWritable
import org.junit.Test
import software.amazon.ionhiveserde.ION
import kotlin.test.assertEquals

class IonIntToSmallIntObjectInspectorTest : AbstractIonIntObjectInspectorTest() {

    protected override val subject = IonIntToSmallIntObjectInspector()
    override val overflowValue = ION.newInt(Long.MAX_VALUE)!!

    @Test
    fun getPrimitiveWritableObject() {
        val ionValue = ION.newInt(10)
        val actual = subject.getPrimitiveWritableObject(ionValue)

        assertEquals(ShortWritable(10), actual)
    }

    @Test
    fun getPrimitiveJavaObject() {
        val ionValue = ION.newInt(10)
        val actual = subject.getPrimitiveJavaObject(ionValue)

        assertEquals(10.toShort(), actual)
    }
}