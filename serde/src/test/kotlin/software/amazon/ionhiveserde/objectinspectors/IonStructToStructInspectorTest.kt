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

package software.amazon.ionhiveserde.objectinspectors

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*
import org.junit.Test
import software.amazon.ion.IonStruct
import software.amazon.ionhiveserde.ION
import kotlin.test.assertEquals
import kotlin.test.assertNull

class IonStructToStructInspectorTest {

    private val subject = IonStructToStructInspector(makeStructInfo(), makeStructObjectInspectors())

    @Test
    fun getAllStructFieldRefs() {
        val actual = subject.allStructFieldRefs

        assertEquals(2, actual.size)

        assertBoolField(actual[0])
        assertIntField(actual[1])
    }

    @Test
    fun getStructFieldRef() {
        assertBoolField(subject.getStructFieldRef("cboolean"))
        assertIntField(subject.getStructFieldRef("cint"))
    }

    @Test(expected = IllegalArgumentException::class)
    fun getStructFieldRefForNull() {
        subject.getStructFieldRef(null)
    }

    @Test
    fun getStructFieldData() {
        val boolField = subject.getStructFieldData(makeStruct(), subject.getStructFieldRef("cboolean"))
        assertEquals(ION.newBool(true), boolField)

        val intField = subject.getStructFieldData(makeStruct(), subject.getStructFieldRef("cint"))
        assertEquals(ION.newInt(1), intField)
    }

    @Test
    fun getStructFieldDataForNullData() {
        val actual = subject.getStructFieldData(null, subject.getStructFieldRef("cint"))
        assertNull(actual)
    }

    @Test(expected = IllegalArgumentException::class)
    fun getStructFieldDataForNullField() {
        subject.getStructFieldData(makeStruct(), null)
    }

    @Test
    fun getStructFieldsDataAsList() {
        val list = subject.getStructFieldsDataAsList(makeStruct())
        assertEquals(ION.newBool(true), list[0])
        assertEquals(ION.newInt(1), list[1])
    }

    @Test
    fun getStructFieldsDataAsListForNull() {
        val actual = subject.getStructFieldsDataAsList(null)
        assertNull(actual)
    }

    @Test
    fun getTypeName() {
        assertEquals("struct<cboolean:boolean,cint:int>", subject.typeName)
    }

    @Test
    fun getCategory() {
        assertEquals(STRUCT, subject.category)
    }

    private fun makeStructInfo(): StructTypeInfo {
        val typeInfo = getStructTypeInfo(
                listOf("cboolean", "cint"),
                listOf(booleanTypeInfo, intTypeInfo))

        return typeInfo as StructTypeInfo
    }

    private fun makeStructObjectInspectors(): List<ObjectInspector> = listOf(IonBooleanToBooleanObjectInspector(), IonIntToIntObjectInspector())

    private fun makeStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        struct.put("cboolean", ION.newBool(true))
        struct.put("cint", ION.newInt(1))

        return struct
    }

    private fun assertBoolField(field: StructField) {
        assertEquals(0, field.fieldID)
        assertEquals("cboolean", field.fieldName)
        assertEquals("boolean", field.fieldObjectInspector.typeName)
        assertEquals("", field.fieldComment)
    }

    private fun assertIntField(field: StructField) {
        assertEquals(1, field.fieldID)
        assertEquals("cint", field.fieldName)
        assertEquals("int", field.fieldObjectInspector.typeName)
        assertEquals("", field.fieldComment)
    }
}