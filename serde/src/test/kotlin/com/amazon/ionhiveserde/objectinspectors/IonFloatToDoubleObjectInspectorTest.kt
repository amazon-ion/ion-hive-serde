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
import org.apache.hadoop.io.DoubleWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonFloatToDoubleObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest() {
    companion object {
        private const val d = 1.123456789
    }

    override val subject = IonFloatToDoubleObjectInspector()

    @Test
    fun getPrimitiveJavaObject() {
        val ionValue = ION.newFloat(d)
        val actual = subject.getPrimitiveJavaObject(ionValue)

        assertEquals(d, actual)
    }

    @Test
    fun getPrimitiveWritableObject() {
        val ionValue = ION.newFloat(d)
        val actual = subject.getPrimitiveWritableObject(ionValue)

        assertEquals(DoubleWritable(d), actual)
    }
}