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
import org.apache.hadoop.hive.common.type.HiveDecimal
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.junit.Test
import kotlin.test.assertEquals

class IonDecimalToDecimalObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest() {

    override val subject = IonDecimalToDecimalObjectInspector()

    @Test
    fun getPrimitiveWritableObject() {
        val ionValue = ION.newDecimal(1)
        val actual = subject.getPrimitiveWritableObject(ionValue)

        assertEquals(HiveDecimalWritable(HiveDecimal.ONE), actual)
    }

    @Test
    fun getPrimitiveJavaObject() {
        val ionValue = ION.newDecimal(1)
        val actual = subject.getPrimitiveJavaObject(ionValue)

        assertEquals(HiveDecimal.ONE, actual)
    }
}

