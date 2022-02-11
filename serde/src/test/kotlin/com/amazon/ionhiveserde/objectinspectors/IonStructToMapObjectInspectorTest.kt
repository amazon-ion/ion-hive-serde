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

import com.amazon.ion.IonStruct
import com.amazon.ionhiveserde.ION
import com.amazon.ionhiveserde.ionNull
import com.amazon.ionhiveserde.objectinspectors.map.*
import org.apache.hadoop.hive.common.type.HiveDecimal
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.MAP
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE
import org.junit.Test
import java.sql.Date
import java.sql.Timestamp
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail

class IonStructToMapObjectInspectorTest {
    private val elementInspector = IonIntToIntObjectInspector(true)

    private val stringIntSubject = IonStructToMapObjectInspector(IonFieldNameToStringObjectInspector(), elementInspector)
    private val intIntSubject = IonStructToMapObjectInspector(IonFieldNameToIntObjectInspector(true), elementInspector)
    private val intIntSubjectOverflow = IonStructToMapObjectInspector(IonFieldNameToIntObjectInspector(false), elementInspector)
    private val bigintIntSubject = IonStructToMapObjectInspector(IonFieldNameToBigIntObjectInspector(), elementInspector)
    private val smallintIntSubject = IonStructToMapObjectInspector(IonFieldNameToSmallIntObjectInspector(true), elementInspector)
    private val tinyintIntSubject = IonStructToMapObjectInspector(IonFieldNameToTinyIntObjectInspector(true), elementInspector)
    private val floatIntSubject = IonStructToMapObjectInspector(IonFieldNameToFloatObjectInspector(true), elementInspector)
    private val doubleIntSubject = IonStructToMapObjectInspector(IonFieldNameToDoubleObjectInspector(), elementInspector)
    private val booleanIntSubject = IonStructToMapObjectInspector(IonFieldNameToBooleanObjectInspector(), elementInspector)
    private val decimalIntSubject = IonStructToMapObjectInspector(IonFieldNameToDecimalObjectInspector(), elementInspector)
    private val dateIntSubject = IonStructToMapObjectInspector(IonFieldNameToDateObjectInspector(), elementInspector)
    private val timestampIntSubject = IonStructToMapObjectInspector(IonFieldNameToTimestampObjectInspector(), elementInspector)


    private val testBigints = listOf<Long>(1L, 2L, 3L)
    private val testBooleans = listOf<Boolean>(true, false)
    private val testDates = listOf<Date>(
            Date.valueOf("1991-1-1"),
            Date.valueOf("1992-2-3"))
    private val testDecimals = listOf<HiveDecimal>(HiveDecimal.create(1), HiveDecimal.create(2))
    private val testDoubles = listOf<Double>(2.0, 4.0, 6.0)
    private val testFloats = listOf<Float>(2f, 4f, 8f)
    private val testInts = listOf<Int>(1, 2, 3)
    private val testSmallints = listOf<Short>(1, 2, 3)
    private val testStrings = listOf<String>("a", "b", "c")
    private val testTimestamps = listOf<Timestamp>(
            Timestamp.valueOf("2014-10-14 12:34:56.789"),
            Timestamp.valueOf("2015-10-16 12:34:56.789"))
    private val testTinyints = listOf<Byte>(1, 2, 3)


    @Test
    fun getMapKeyObjectInspector() {
        assertEquals(stringIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(intIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(intIntSubjectOverflow.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(bigintIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(smallintIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(tinyintIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(floatIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(doubleIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(booleanIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(decimalIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(dateIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(timestampIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
    }

    @Test
    fun getMapValueObjectInspector() {
        assertEquals(elementInspector, stringIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, intIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, intIntSubjectOverflow.mapValueObjectInspector)
        assertEquals(elementInspector, bigintIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, smallintIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, tinyintIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, floatIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, doubleIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, booleanIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, decimalIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, dateIntSubject.mapValueObjectInspector)
        assertEquals(elementInspector, timestampIntSubject.mapValueObjectInspector)
    }

    @Test
    fun getMapValueElement() {
        val structs = makeStructs()
        testGetMapValueElement(structs[0], stringIntSubject, testStrings)
        testGetMapValueElement(structs[1], intIntSubject, testInts.map { it.toString() })
        testGetMapValueElement(structs[1], intIntSubjectOverflow, testInts.map { it.toString() })
        testGetMapValueElement(structs[2], bigintIntSubject, testBigints.map { it.toString() })
        testGetMapValueElement(structs[3], smallintIntSubject, testSmallints.map { it.toString() })
        testGetMapValueElement(structs[4], tinyintIntSubject, testTinyints.map { it.toString() })
        testGetMapValueElement(structs[5], floatIntSubject, testFloats.map { it.toString() })
        testGetMapValueElement(structs[6], doubleIntSubject, testDoubles.map { it.toString() })
        testGetMapValueElement(structs[7], booleanIntSubject, testBooleans.map { it.toString() })
        testGetMapValueElement(structs[8], decimalIntSubject, testDecimals.map { it.toString() })
        testGetMapValueElement(structs[9], dateIntSubject, testDates.map { it.toString() })
        testGetMapValueElement(structs[10], timestampIntSubject, testTimestamps.map { getTsKey(it) })
    }

    private fun testGetMapValueElement(struct: IonStruct, subject : IonStructToMapObjectInspector, keys: List<String>) {
        var structVal = 1
        keys.forEach{assertEquals(ION.newInt(structVal++), subject.getMapValueElement(struct, ION.newSymbol(it)))}
        assertNull(subject.getMapValueElement(struct, ION.newSymbol("4")))
    }

    @Test
    fun getMapValueElementForNullData() {
        assertNull(stringIntSubject.getMapValueElement(null, ION.newSymbol("a")))
        assertNull(stringIntSubject.getMapValueElement(ionNull, ION.newSymbol("a")))

        assertNull(intIntSubject.getMapValueElement(null, ION.newSymbol("1")))
        assertNull(intIntSubject.getMapValueElement(ionNull, ION.newSymbol("1")))

        assertNull(bigintIntSubject.getMapValueElement(null, ION.newSymbol("1")))
        assertNull(bigintIntSubject.getMapValueElement(ionNull, ION.newSymbol("1")))

        assertNull(smallintIntSubject.getMapValueElement(null, ION.newSymbol("1")))
        assertNull(smallintIntSubject.getMapValueElement(ionNull, ION.newSymbol("1")))

        assertNull(tinyintIntSubject.getMapValueElement(null, ION.newSymbol("1")))
        assertNull(tinyintIntSubject.getMapValueElement(ionNull, ION.newSymbol("1")))

        val floatVal = 2f;
        assertNull(floatIntSubject.getMapValueElement(null, ION.newSymbol(floatVal.toString())))
        assertNull(floatIntSubject.getMapValueElement(ionNull, ION.newSymbol(floatVal.toString())))

        val doubleVal = 2.0;
        assertNull(doubleIntSubject.getMapValueElement(null, ION.newSymbol(doubleVal.toString())))
        assertNull(doubleIntSubject.getMapValueElement(ionNull, ION.newSymbol(doubleVal.toString())))

        // No boolean check here because both true and false are already used

        val decimalVal = HiveDecimal.create(3)
        assertNull(decimalIntSubject.getMapValueElement(null, ION.newSymbol(decimalVal.toString())))
        assertNull(decimalIntSubject.getMapValueElement(ionNull, ION.newSymbol(decimalVal.toString())))

        val dateVal = Date.valueOf("1998-1-2")
        assertNull(dateIntSubject.getMapValueElement(null, ION.newSymbol(dateVal.toString())))
        assertNull(dateIntSubject.getMapValueElement(ionNull, ION.newSymbol(dateVal.toString())))

        val timestampVal = Timestamp.valueOf("2015-10-16 12:34:56.887")
        assertNull(timestampIntSubject.getMapValueElement(null, ION.newSymbol(timestampVal.toString())))
        assertNull(timestampIntSubject.getMapValueElement(ionNull, ION.newSymbol(timestampVal.toString())))
    }

    @Test
    fun getMapValueElementForNullKey() {
        val structs = makeStructs()
        assertNullThrowsIllegalArgumentException(stringIntSubject, structs[0])
        assertNullThrowsIllegalArgumentException(intIntSubject, structs[1])
        assertNullThrowsIllegalArgumentException(bigintIntSubject, structs[2])
        assertNullThrowsIllegalArgumentException(smallintIntSubject, structs[3])
        assertNullThrowsIllegalArgumentException(tinyintIntSubject, structs[4])
        assertNullThrowsIllegalArgumentException(floatIntSubject, structs[5])
        assertNullThrowsIllegalArgumentException(doubleIntSubject, structs[6])
        assertNullThrowsIllegalArgumentException(booleanIntSubject, structs[7])
        assertNullThrowsIllegalArgumentException(decimalIntSubject, structs[8])
        assertNullThrowsIllegalArgumentException(dateIntSubject, structs[9])
        assertNullThrowsIllegalArgumentException(timestampIntSubject, structs[10])
    }

    private fun assertNullThrowsIllegalArgumentException(subject: IonStructToMapObjectInspector, struct: IonStruct) {
        try {
            assertNull(subject.getMapValueElement(struct, null))
            fail("IllegalArgumentException expected")
        } catch (e: IllegalArgumentException) {
            // success
        } catch (e: Exception) {
            fail("Expected IllegalArgumentException, caught $e")
        }
    }

    @Test
    fun getMap() {
        val structs = makeStructs()
        testGetMaps(structs[0], stringIntSubject, testStrings)
        testGetMaps(structs[1], intIntSubject, testInts)
        testGetMaps(structs[1], intIntSubjectOverflow, testInts)
        testGetMaps(structs[2], bigintIntSubject, testBigints)
        testGetMaps(structs[3], smallintIntSubject, testSmallints)
        testGetMaps(structs[4], tinyintIntSubject, testTinyints)
        testGetMaps(structs[5], floatIntSubject, testFloats)
        testGetMaps(structs[6], doubleIntSubject, testDoubles)
        testGetMaps(structs[7], booleanIntSubject, testBooleans)
        testGetMaps(structs[8], decimalIntSubject, testDecimals)
        testGetMaps(structs[9], dateIntSubject, testDates)
        testGetMaps(structs[10], timestampIntSubject, testTimestamps)
    }

    private fun testGetMaps(struct : IonStruct, subject : IonStructToMapObjectInspector, keyValues : List<Any>) {
        val map = subject.getMap(struct)
        assertEquals(keyValues.size, map.size)
        var ionVal = 1
        keyValues.forEach{assertEquals(ION.newInt(ionVal++), map[it])}
    }

    @Test
    fun getMapForNullData() {
        testGetMapForNullData(stringIntSubject)

        testGetMapForNullData(intIntSubjectOverflow)
        testGetMapForNullData(bigintIntSubject)
        testGetMapForNullData(smallintIntSubject)
        testGetMapForNullData(tinyintIntSubject)

        testGetMapForNullData(floatIntSubject)
        testGetMapForNullData(doubleIntSubject)
        testGetMapForNullData(decimalIntSubject)

        testGetMapForNullData(booleanIntSubject)

        testGetMapForNullData(dateIntSubject)
        testGetMapForNullData(timestampIntSubject)
    }

    private fun testGetMapForNullData(subject : IonStructToMapObjectInspector) {
        assertNull(subject.getMap(null))
        assertNull(subject.getMap(ionNull))
    }

    @Test
    fun getMapSize() {
        val structs = makeStructs()
        assertEquals(3, stringIntSubject.getMapSize(structs[0]))
        assertEquals(3, intIntSubject.getMapSize(structs[1]))
        assertEquals(3, intIntSubjectOverflow.getMapSize(structs[1]))
        assertEquals(3, bigintIntSubject.getMapSize(structs[2]))
        assertEquals(3, smallintIntSubject.getMapSize(structs[3]))
        assertEquals(3, tinyintIntSubject.getMapSize(structs[4]))
        assertEquals(3, floatIntSubject.getMapSize(structs[5]))
        assertEquals(3, doubleIntSubject.getMapSize(structs[6]))
        assertEquals(2, booleanIntSubject.getMapSize(structs[7]))
        assertEquals(2, decimalIntSubject.getMapSize(structs[8]))
        assertEquals(2, dateIntSubject.getMapSize(structs[9]))
        assertEquals(2, timestampIntSubject.getMapSize(structs[10]))
    }

    @Test
    fun getMapSizeForNull() {
        testGetMapSizeForNull(stringIntSubject)

        testGetMapSizeForNull(intIntSubjectOverflow)
        testGetMapSizeForNull(bigintIntSubject)
        testGetMapSizeForNull(smallintIntSubject)
        testGetMapSizeForNull(tinyintIntSubject)

        testGetMapSizeForNull(floatIntSubject)
        testGetMapSizeForNull(doubleIntSubject)
        testGetMapSizeForNull(decimalIntSubject)

        testGetMapSizeForNull(booleanIntSubject)

        testGetMapSizeForNull(dateIntSubject)
        testGetMapSizeForNull(timestampIntSubject)
    }

    private fun testGetMapSizeForNull(subject : IonStructToMapObjectInspector) {
        assertEquals(-1, subject.getMapSize(null))
        assertEquals(-1, subject.getMapSize(ionNull))
    }

    @Test
    fun getTypeName() {
        assertEquals("map<string,int>", stringIntSubject.typeName)
        assertEquals("map<int,int>", intIntSubject.typeName)
        assertEquals("map<bigint,int>", bigintIntSubject.typeName)
        assertEquals("map<smallint,int>", smallintIntSubject.typeName)
        assertEquals("map<tinyint,int>", tinyintIntSubject.typeName)
        assertEquals("map<float,int>", floatIntSubject.typeName)
        assertEquals("map<double,int>", doubleIntSubject.typeName)
        assertEquals("map<boolean,int>", booleanIntSubject.typeName)
        assertEquals("map<decimal(38,18),int>", decimalIntSubject.typeName)
        assertEquals("map<date,int>", dateIntSubject.typeName)
        assertEquals("map<timestamp,int>", timestampIntSubject.typeName)
    }

    @Test
    fun getCategory() {
        assertEquals(MAP, stringIntSubject.category)
        assertEquals(MAP, intIntSubject.category)
        assertEquals(MAP, bigintIntSubject.category)
        assertEquals(MAP, smallintIntSubject.category)
        assertEquals(MAP, tinyintIntSubject.category)
        assertEquals(MAP, floatIntSubject.category)
        assertEquals(MAP, doubleIntSubject.category)
        assertEquals(MAP, booleanIntSubject.category)
        assertEquals(MAP, decimalIntSubject.category)
        assertEquals(MAP, dateIntSubject.category)
        assertEquals(MAP, timestampIntSubject.category)
    }

    private fun makeStructs(): List<IonStruct> {
        return listOf<IonStruct>(
                generateStruct(testStrings),
                generateStruct(testInts.map { it.toString() }),
                generateStruct(testBigints.map { it.toString() }),
                generateStruct(testSmallints.map { it.toString() }),
                generateStruct(testTinyints.map { it.toString() }),
                generateStruct(testFloats.map { it.toString() }),
                generateStruct(testDoubles.map { it.toString() }),
                generateStruct(testBooleans.map { it.toString() }),
                generateStruct(testDecimals.map { it.toString() }),
                generateStruct(testDates.map { it.toString() }),
                generateStruct(testTimestamps.map { getTsKey(it) }))
    }

    private fun getTsKey(timestamp: Timestamp): String {
        return com.amazon.ion.Timestamp.forSqlTimestampZ(timestamp).toString()
    }

    private fun generateStruct(keys : List<String>): IonStruct {
        val struct = ION.newEmptyStruct();
        var structVal = 1
        keys.stream().forEach{struct.add(it, ION.newInt(structVal++))}
        return struct
    }

}