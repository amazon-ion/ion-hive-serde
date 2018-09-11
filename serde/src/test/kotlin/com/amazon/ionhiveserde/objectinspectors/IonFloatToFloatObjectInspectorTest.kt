/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ionhiveserde.ION
import org.apache.hadoop.io.FloatWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonFloatToFloatObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest() {

    override val subject = IonFloatToFloatObjectInspector()

    @Test
    fun getPrimitiveJavaObject() {
        val ionValue = ION.newFloat(10)
        val actual = subject.getPrimitiveJavaObject(ionValue)

        assertEquals(10.toFloat(), actual)
    }

    @Test(expected = IllegalArgumentException::class)
    fun getPrimitiveJavaObjectOverflow() {
        subject.getPrimitiveJavaObject(ION.newFloat(1.123456789))
    }

    @Test
    fun getPrimitiveWritableObject() {
        val ionValue = ION.newFloat(10)
        val actual = subject.getPrimitiveWritableObject(ionValue)

        assertEquals(FloatWritable(10.toFloat()), actual)
    }

    @Test(expected = IllegalArgumentException::class)
    fun getPrimitiveWritableObjectOverflow() {
        subject.getPrimitiveWritableObject(ION.newFloat(1.123456789))
    }
}

