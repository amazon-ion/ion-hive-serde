/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ionhiveserde.ION
import com.amazon.ionhiveserde.case_insensitive
import com.amazon.ionhiveserde.ionNull
import com.amazon.ionhiveserde.struct_for
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.MAP
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class IonStructToMapObjectInspectorTest {
    private val valueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val subject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(valueElementInspector)

    @Test
    fun getMapKeyObjectInspector() {
        assertEquals(PrimitiveObjectInspectorFactory.javaStringObjectInspector, subject.mapKeyObjectInspector)
    }

    @Test
    fun getMapValueObjectInspector() {
        assertEquals(valueElementInspector, subject.mapValueObjectInspector)
    }

    @Test
    fun getMapValueElement() {
        val struct = struct_for("{a: 1, b: 2, c: null}")

        assertEquals(ION.newInt(1), subject.getMapValueElement(struct, ION.newSymbol("a")))
        assertEquals(ION.newInt(2), subject.getMapValueElement(struct, ION.newSymbol("b")))
        assertEquals(null, subject.getMapValueElement(struct, ION.newSymbol("c")))
        assertNull(subject.getMapValueElement(struct, ION.newSymbol("d")))
    }

    @Test
    fun getMapValueElementForNullData() {
        assertNull(subject.getMapValueElement(null, ION.newSymbol("a")))
        assertNull(subject.getMapValueElement(ionNull, ION.newSymbol("a")))
    }

    @Test(expected = IllegalArgumentException::class)
    fun getMapValueElementForNullKey() {
        assertNull(subject.getMapValueElement(struct_for("{a: 1, b: 2, c: 3}"), null))
    }

    @Test
    fun getMapSize() {
        assertEquals(3, subject.getMapSize(struct_for("{a: 1, b: 2, c: null}")))
    }

    @Test
    fun getMap() {
        val struct = struct_for("{a: 1, b: 2, c: null}")
        val actual = subject.getMap(struct)

        // We should make sure the null value is inside the map returned by getMap()
        assertEquals(3, actual.size)

        assertEquals(ION.newInt(1), actual["a"])
        assertEquals(ION.newInt(2), actual["b"])
        assertEquals(null, actual["c"])
    }

    @Test
    fun getMapSizeForCaseInsensitiveDecorator() {
        assertEquals(2, subject.getMapSize(case_insensitive(struct_for("{a: 1, b: null}"))))
    }

    @Test
    fun getMapForCaseInsensitiveDecorator() {
        val struct = case_insensitive(struct_for("{a: 1, b: null}"))
        val actual = subject.getMap(struct)

        // We should make sure the null value is inside the map returned by getMap()
        assertEquals(2, actual.size)

        assertEquals(ION.newInt(1), actual["a"])
        assertEquals(null, actual["b"])
    }

    @Test
    fun getMapForNullData() {
        assertNull(subject.getMap(null))
        assertNull(subject.getMap(ionNull))
    }

    @Test
    fun getMapSizeForNull() {
        assertEquals(-1, subject.getMapSize(null))
        assertEquals(-1, subject.getMapSize(ionNull))
    }

    @Test
    fun getTypeName() {
        assertEquals("map<string,int>", subject.typeName)
    }

    @Test
    fun getCategory() {
        assertEquals(MAP, subject.category)
    }
}
