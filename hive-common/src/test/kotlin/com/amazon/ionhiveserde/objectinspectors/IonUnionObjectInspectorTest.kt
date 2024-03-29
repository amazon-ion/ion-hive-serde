// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ionhiveserde.ION
import com.amazon.ionhiveserde.ionNull
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.UNION
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class IonUnionObjectInspectorTest {

    private val objectInspectors = listOf(com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true), com.amazon.ionhiveserde.objectinspectors.IonBooleanToBooleanObjectInspector())
    private val subject = com.amazon.ionhiveserde.objectinspectors.IonUnionObjectInspector(objectInspectors)

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
