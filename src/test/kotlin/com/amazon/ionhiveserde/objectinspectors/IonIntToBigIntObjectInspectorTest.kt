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
import org.apache.hadoop.io.LongWritable
import org.junit.Test
import java.math.BigInteger
import kotlin.test.assertEquals

class IonIntToBigIntObjectInspectorTest : AbstractIonIntObjectInspectorTest() {

    protected override val subject = IonIntToBigIntObjectInspector()
    override val overflowValue = ION.newInt(BigInteger.valueOf(Long.MAX_VALUE).inc())!!

    @Test
    fun getPrimitiveWritableObject() {
        val ionValue = ION.newInt(10)
        val actual = subject.getPrimitiveWritableObject(ionValue)

        assertEquals(LongWritable(10), actual)
    }

    @Test
    fun getPrimitiveJavaObject() {
        val ionValue = ION.newInt(10)
        val actual = subject.getPrimitiveJavaObject(ionValue)

        assertEquals(10.toLong(), actual)
    }
}