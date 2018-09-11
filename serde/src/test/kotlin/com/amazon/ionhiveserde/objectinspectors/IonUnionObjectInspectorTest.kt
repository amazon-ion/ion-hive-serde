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
import com.amazon.ionhiveserde.ionNull
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.UNION
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class IonUnionObjectInspectorTest {

    private val objectInspectors = listOf(IonIntToIntObjectInspector(), IonBooleanToBooleanObjectInspector())
    private val subject = IonUnionObjectInspector(objectInspectors)

    @Test
    fun getObjectInspectors() {
        assertEquals(objectInspectors, subject.objectInspectors)
    }

    @Test
    fun getTag() {
        val int = ION.newInt(1)
        val bool = ION.newBool(true)

        assertEquals(0, subject.getTag(int))
        assertEquals(1, subject.getTag(bool))
    }

    @Test
    fun getTagForNull() {
        assertEquals(0, subject.getTag(null))
        assertEquals(0, subject.getTag(ionNull))
    }

    @Test
    fun getField() {
        val int = ION.newInt(1)
        assertEquals(int, subject.getField(int))
    }

    @Test
    fun getFieldForNull() {
        assertNull(subject.getField(null))
        assertNull(subject.getField(ionNull))
    }

    @Test
    fun getTypeName() {
        assertEquals("uniontype<int,boolean>", subject.typeName)
    }

    @Test
    fun getCategory() {
        assertEquals(UNION, subject.category)
    }
}