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
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.MAP
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE
import org.junit.Test
import java.sql.Date
import java.sql.Timestamp
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.fail

class IonStructToMapObjectInspectorTest {
    private val stringIntKeyElementInspector = IonFieldNameToStringObjectInspector(true)
    private val stringIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val stringIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(stringIntKeyElementInspector, stringIntValueElementInspector)

    private val intIntKeyElementInspector = IonFieldNameToIntObjectInspector(true)
    private val intIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val intIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(intIntKeyElementInspector, intIntValueElementInspector)

    private val bigintIntKeyElementInspector = IonFieldNameToBigIntObjectInspector(true)
    private val bigintIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val bigintIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(bigintIntKeyElementInspector, bigintIntValueElementInspector)

    private val smallintIntKeyElementInspector = IonFieldNameToSmallIntObjectInspector(true)
    private val smallintIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val smallintIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(smallintIntKeyElementInspector, smallintIntValueElementInspector)

    private val tinyintIntKeyElementInspector = IonFieldNameToTinyIntObjectInspector(true)
    private val tinyintIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val tinyintIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(tinyintIntKeyElementInspector, tinyintIntValueElementInspector)

    private val floatIntKeyElementInspector = IonFieldNameToFloatObjectInspector(true)
    private val floatIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val floatIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(floatIntKeyElementInspector, floatIntValueElementInspector)

    private val doubleIntKeyElementInspector = IonFieldNameToDoubleObjectInspector(true)
    private val doubleIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val doubleIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(doubleIntKeyElementInspector, doubleIntValueElementInspector)

    private val booleanIntKeyElementInspector = IonFieldNameToBooleanObjectInspector(true)
    private val booleanIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val booleanIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(booleanIntKeyElementInspector, booleanIntValueElementInspector)

    private val decimalIntKeyElementInspector = IonFieldNameToDecimalObjectInspector(true)
    private val decimalIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val decimalIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(decimalIntKeyElementInspector, decimalIntValueElementInspector)

    private val dateIntKeyElementInspector = IonFieldNameToDateObjectInspector(true)
    private val dateIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val dateIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(dateIntKeyElementInspector, dateIntValueElementInspector)

    private val timestampIntKeyElementInspector = IonFieldNameToTimestampObjectInspector(true)
    private val timestampIntValueElementInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val timestampIntSubject = com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector(timestampIntKeyElementInspector, timestampIntValueElementInspector)

    @Test
    fun getMapKeyObjectInspector() {
        assertEquals(stringIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
        assertEquals(intIntSubject.mapKeyObjectInspector.category, PRIMITIVE)
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
        assertEquals(stringIntValueElementInspector, stringIntSubject.mapValueObjectInspector)
        assertEquals(intIntValueElementInspector, intIntSubject.mapValueObjectInspector)
        assertEquals(bigintIntValueElementInspector, bigintIntSubject.mapValueObjectInspector)
        assertEquals(smallintIntValueElementInspector, smallintIntSubject.mapValueObjectInspector)
        assertEquals(tinyintIntValueElementInspector, tinyintIntSubject.mapValueObjectInspector)
        assertEquals(floatIntValueElementInspector, floatIntSubject.mapValueObjectInspector)
        assertEquals(doubleIntValueElementInspector, doubleIntSubject.mapValueObjectInspector)
        assertEquals(booleanIntValueElementInspector, booleanIntSubject.mapValueObjectInspector)
        assertEquals(decimalIntValueElementInspector, decimalIntSubject.mapValueObjectInspector)
        assertEquals(dateIntValueElementInspector, dateIntSubject.mapValueObjectInspector)
        assertEquals(timestampIntValueElementInspector, timestampIntSubject.mapValueObjectInspector)
    }

    @Test
    fun getMapValueElement() {
        val structs = makeStructs()
        val stringIntStruct = structs[0]

        assertEquals(ION.newInt(1), stringIntSubject.getMapValueElement(stringIntStruct, ION.newSymbol("a")))
        assertEquals(ION.newInt(2), stringIntSubject.getMapValueElement(stringIntStruct, ION.newSymbol("b")))
        assertEquals(ION.newInt(3), stringIntSubject.getMapValueElement(stringIntStruct, ION.newSymbol("c")))
        assertNull(stringIntSubject.getMapValueElement(stringIntStruct, ION.newSymbol("d")))

        val intIntStruct = structs[1]
        assertEquals(ION.newInt(1), intIntSubject.getMapValueElement(intIntStruct, ION.newSymbol("1")))
        assertEquals(ION.newInt(2), intIntSubject.getMapValueElement(intIntStruct, ION.newSymbol("2")))
        assertEquals(ION.newInt(3), intIntSubject.getMapValueElement(intIntStruct, ION.newSymbol("3")))
        assertNull(intIntSubject.getMapValueElement(intIntStruct, ION.newSymbol("4")))

        val bigintIntStruct = structs[2]
        assertEquals(ION.newInt(1), bigintIntSubject.getMapValueElement(bigintIntStruct, ION.newSymbol("1")))
        assertEquals(ION.newInt(2), bigintIntSubject.getMapValueElement(bigintIntStruct, ION.newSymbol("2")))
        assertEquals(ION.newInt(3), bigintIntSubject.getMapValueElement(bigintIntStruct, ION.newSymbol("3")))
        assertNull(bigintIntSubject.getMapValueElement(bigintIntStruct, ION.newSymbol("4")))

        val smallintIntStruct = structs[3]
        assertEquals(ION.newInt(1), smallintIntSubject.getMapValueElement(smallintIntStruct, ION.newSymbol("1")))
        assertEquals(ION.newInt(2), smallintIntSubject.getMapValueElement(smallintIntStruct, ION.newSymbol("2")))
        assertEquals(ION.newInt(3), smallintIntSubject.getMapValueElement(smallintIntStruct, ION.newSymbol("3")))
        assertNull(smallintIntSubject.getMapValueElement(smallintIntStruct, ION.newSymbol("4")))

        val tinyintIntStruct = structs[4]
        assertEquals(ION.newInt(1), tinyintIntSubject.getMapValueElement(tinyintIntStruct, ION.newSymbol("1")))
        assertEquals(ION.newInt(2), tinyintIntSubject.getMapValueElement(tinyintIntStruct, ION.newSymbol("2")))
        assertEquals(ION.newInt(3), tinyintIntSubject.getMapValueElement(tinyintIntStruct, ION.newSymbol("3")))
        assertNull(tinyintIntSubject.getMapValueElement(tinyintIntStruct, ION.newSymbol("4")))

        val floatIntStruct = structs[5]
        val floatList = listOf<Float>(2f, 4f, 8f, 16f)
        assertEquals(ION.newInt(1), floatIntSubject.getMapValueElement(floatIntStruct, ION.newSymbol(floatList[0].toString())))
        assertEquals(ION.newInt(2), floatIntSubject.getMapValueElement(floatIntStruct, ION.newSymbol(floatList[1].toString())))
        assertEquals(ION.newInt(3), floatIntSubject.getMapValueElement(floatIntStruct, ION.newSymbol(floatList[2].toString())))
        assertNull(floatIntSubject.getMapValueElement(floatIntStruct, ION.newSymbol(floatList[3].toString())))

        val doubleIntStruct = structs[6]
        val doubleList = listOf<Double>(2.0, 4.0, 6.0, 8.0)
        assertEquals(ION.newInt(1), doubleIntSubject.getMapValueElement(doubleIntStruct, ION.newSymbol(doubleList[0].toString())))
        assertEquals(ION.newInt(2), doubleIntSubject.getMapValueElement(doubleIntStruct, ION.newSymbol(doubleList[1].toString())))
        assertEquals(ION.newInt(3), doubleIntSubject.getMapValueElement(doubleIntStruct, ION.newSymbol(doubleList[2].toString())))
        assertNull(doubleIntSubject.getMapValueElement(doubleIntStruct, ION.newSymbol(doubleList[3].toString())))

        val boolIntStruct = structs[7]
        val boolList = listOf<Boolean>(true, false)
        assertEquals(ION.newInt(1), booleanIntSubject.getMapValueElement(boolIntStruct, ION.newSymbol(boolList[0].toString())))
        assertEquals(ION.newInt(2), booleanIntSubject.getMapValueElement(boolIntStruct, ION.newSymbol(boolList[1].toString())))

        val decimalIntStruct = structs[8]
        val decimalList = listOf<HiveDecimal>(HiveDecimal.create(1), HiveDecimal.create(2))
        assertEquals(ION.newInt(1), decimalIntSubject.getMapValueElement(decimalIntStruct, ION.newSymbol(decimalList[0].toString())))
        assertEquals(ION.newInt(2), decimalIntSubject.getMapValueElement(decimalIntStruct, ION.newSymbol(decimalList[1].toString())))

        val dateIntStruct = structs[9]
        val dateList = listOf<Date>(
                Date.valueOf("1991-1-1"),
                Date.valueOf("1992-2-3"))
        assertEquals(ION.newInt(1), dateIntSubject.getMapValueElement(dateIntStruct, ION.newSymbol(dateList[0].toString())))
        assertEquals(ION.newInt(2), dateIntSubject.getMapValueElement(dateIntStruct, ION.newSymbol(dateList[1].toString())))

        val timestampIntStruct = structs[10]
        val tsList = listOf<Timestamp>(
                Timestamp.valueOf("2014-10-14 12:34:56.789"),
                Timestamp.valueOf("2015-10-16 12:34:56.789"))
        assertEquals(ION.newInt(1), timestampIntSubject.getMapValueElement(timestampIntStruct, ION.newSymbol(tsList[0].toString())))
        assertEquals(ION.newInt(2), timestampIntSubject.getMapValueElement(timestampIntStruct, ION.newSymbol(tsList[1].toString())))
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

    private fun assertNullThrowsIllegalArgumentException (subject: IonStructToMapObjectInspector, struct: IonStruct) {
        try {
            assertNull(subject.getMapValueElement(struct, null))
            fail("IllegalArgumentException expected")
        } catch (e: IllegalArgumentException ) {
            // success
        } catch (e: Exception) {
            fail("Expected IllegalArgumentException, caught $e")
        }
    }

    @Test
    fun getMap() {
        val structs = makeStructs()

        val stringIntStruct = structs[0]
        val stringIntActual = stringIntSubject.getMap(stringIntStruct)
        assertEquals(3, stringIntActual.size)
        assertEquals(ION.newInt(1), stringIntActual["a"])
        assertEquals(ION.newInt(2), stringIntActual["b"])
        assertEquals(ION.newInt(3), stringIntActual["c"])

        val intIntStruct = structs[1]
        val intIntActual = intIntSubject.getMap(intIntStruct)
        assertEquals(3, intIntActual.size)
        assertEquals(ION.newInt(1), intIntActual[1])
        assertEquals(ION.newInt(2), intIntActual[2])
        assertEquals(ION.newInt(3), intIntActual[3])

        val bigintIntStruct = structs[2]
        val bigintIntActual = bigintIntSubject.getMap(bigintIntStruct)
        assertEquals(3, bigintIntActual.size)
        assertEquals(ION.newInt(1), bigintIntActual[1L])
        assertEquals(ION.newInt(2), bigintIntActual[2L])
        assertEquals(ION.newInt(3), bigintIntActual[3L])

        val smallintIntStruct = structs[3]
        val smallintIntActual = smallintIntSubject.getMap(smallintIntStruct)
        val smallintList = listOf<Short>(1, 2, 3)
        assertEquals(3, smallintIntActual.size)
        assertEquals(ION.newInt(1), smallintIntActual[smallintList[0]])
        assertEquals(ION.newInt(2), smallintIntActual[smallintList[1]])
        assertEquals(ION.newInt(3), smallintIntActual[smallintList[2]])

        val tinyintIntStruct = structs[4]
        val tinyintIntActual = tinyintIntSubject.getMap(tinyintIntStruct)
        assertEquals(3, tinyintIntActual.size)
        val tinyintList = listOf<Byte>(1, 2, 3)
        assertEquals(ION.newInt(1), tinyintIntActual[tinyintList[0]])
        assertEquals(ION.newInt(2), tinyintIntActual[tinyintList[1]])
        assertEquals(ION.newInt(3), tinyintIntActual[tinyintList[2]])

        val floatIntStruct = structs[5]
        val floatIntActual = floatIntSubject.getMap(floatIntStruct)
        assertEquals(3, floatIntActual.size)
        val floatList = listOf<Float>(2f, 4f, 8f)
        assertEquals(ION.newInt(1), floatIntActual[floatList[0]])
        assertEquals(ION.newInt(2), floatIntActual[floatList[1]])
        assertEquals(ION.newInt(3), floatIntActual[floatList[2]])

        val doubleIntStruct = structs[6]
        val doubleIntActual = doubleIntSubject.getMap(doubleIntStruct)
        assertEquals(3, doubleIntActual.size)
        val doubleList = listOf<Double>(2.0, 4.0, 6.0)
        assertEquals(ION.newInt(1), doubleIntActual[doubleList[0]])
        assertEquals(ION.newInt(2), doubleIntActual[doubleList[1]])
        assertEquals(ION.newInt(3), doubleIntActual[doubleList[2]])

        val booleanIntStruct = structs[7]
        val booleanIntActual = booleanIntSubject.getMap(booleanIntStruct)
        assertEquals(2, booleanIntActual.size)
        val booleanList = listOf<Boolean>(true, false)
        assertEquals(ION.newInt(1), booleanIntActual[booleanList[0]])
        assertEquals(ION.newInt(2), booleanIntActual[booleanList[1]])

        val decimalIntStruct = structs[8]
        val decimalIntActual = decimalIntSubject.getMap(decimalIntStruct)
        assertEquals(2, decimalIntActual.size)
        val decimalList = listOf<HiveDecimal>(HiveDecimal.create(1), HiveDecimal.create(2))
        assertEquals(ION.newInt(1), decimalIntActual[decimalList[0]])
        assertEquals(ION.newInt(2), decimalIntActual[decimalList[1]])

        val dateIntStruct = structs[9]
        val dateIntActual = dateIntSubject.getMap(dateIntStruct)
        assertEquals(2, dateIntActual.size)
        val dateList = listOf<Date>(
                Date.valueOf("1991-1-1"),
                Date.valueOf("1992-2-3"))
        assertEquals(ION.newInt(1), dateIntActual[dateList[0]])
        assertEquals(ION.newInt(2), dateIntActual[dateList[1]])

        val timestampIntStruct = structs[10]
        val timestampIntActual = timestampIntSubject.getMap(timestampIntStruct)
        assertEquals(2, timestampIntActual.size)
        val tsList = listOf<Timestamp>(
                Timestamp.valueOf("2014-10-14 12:34:56.789"),
                Timestamp.valueOf("2015-10-16 12:34:56.789"))
        assertEquals(ION.newInt(1), timestampIntActual[tsList[0]])
        assertEquals(ION.newInt(2), timestampIntActual[tsList[1]])
    }

    @Test
    fun getMapForNullData() {
        assertNull(stringIntSubject.getMap(null))
        assertNull(stringIntSubject.getMap(ionNull))

        assertNull(intIntSubject.getMap(null))
        assertNull(intIntSubject.getMap(ionNull))

        assertNull(bigintIntSubject.getMap(null))
        assertNull(bigintIntSubject.getMap(ionNull))

        assertNull(smallintIntSubject.getMap(null))
        assertNull(smallintIntSubject.getMap(ionNull))

        assertNull(tinyintIntSubject.getMap(null))
        assertNull(tinyintIntSubject.getMap(ionNull))

        assertNull(floatIntSubject.getMap(null))
        assertNull(floatIntSubject.getMap(ionNull))

        assertNull(doubleIntSubject.getMap(null))
        assertNull(doubleIntSubject.getMap(ionNull))

        assertNull(booleanIntSubject.getMap(null))
        assertNull(booleanIntSubject.getMap(ionNull))

        assertNull(decimalIntSubject.getMap(null))
        assertNull(decimalIntSubject.getMap(ionNull))

        assertNull(dateIntSubject.getMap(null))
        assertNull(dateIntSubject.getMap(ionNull))

        assertNull(timestampIntSubject.getMap(null))
        assertNull(timestampIntSubject.getMap(ionNull))
    }

    @Test
    fun getMapSize() {
        val structs = makeStructs()
        assertEquals(3, stringIntSubject.getMapSize(structs[0]))
        assertEquals(3, intIntSubject.getMapSize(structs[1]))
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
        assertEquals(-1, stringIntSubject.getMapSize(null))
        assertEquals(-1, stringIntSubject.getMapSize(ionNull))

        assertEquals(-1, intIntSubject.getMapSize(null))
        assertEquals(-1, intIntSubject.getMapSize(ionNull))

        assertEquals(-1, bigintIntSubject.getMapSize(null))
        assertEquals(-1, bigintIntSubject.getMapSize(ionNull))

        assertEquals(-1, smallintIntSubject.getMapSize(null))
        assertEquals(-1, smallintIntSubject.getMapSize(ionNull))

        assertEquals(-1, tinyintIntSubject.getMapSize(null))
        assertEquals(-1, tinyintIntSubject.getMapSize(ionNull))

        assertEquals(-1, floatIntSubject.getMapSize(null))
        assertEquals(-1, floatIntSubject.getMapSize(ionNull))

        assertEquals(-1, doubleIntSubject.getMapSize(null))
        assertEquals(-1, doubleIntSubject.getMapSize(ionNull))

        assertEquals(-1, booleanIntSubject.getMapSize(null))
        assertEquals(-1, booleanIntSubject.getMapSize(ionNull))

        assertEquals(-1, decimalIntSubject.getMapSize(null))
        assertEquals(-1, decimalIntSubject.getMapSize(ionNull))

        assertEquals(-1, dateIntSubject.getMapSize(null))
        assertEquals(-1, dateIntSubject.getMapSize(ionNull))

        assertEquals(-1, timestampIntSubject.getMapSize(null))
        assertEquals(-1, timestampIntSubject.getMapSize(ionNull))
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
                makeStringIntStruct(),
                makeIntIntStruct(),
                makeBigintIntStruct(),
                makeSmallintIntStruct(),
                makeTinyintIntStruct(),
                makeFloatIntStruct(),
                makeDoubleIntStruct(),
                makeBoolIntStruct(),
                makeDecimalIntStruct(),
                makeDateIntStruct(),
                makeTimestampIntStruct())
    }

    private fun makeStringIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()

        struct.add("a", ION.newInt(1))
        struct.add("b", ION.newInt(2))
        struct.add("c", ION.newInt(3))

        return struct
    }

    private fun makeIntIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val intList = listOf<Int>(1, 2, 3)

        struct.add(intList[0].toString(), ION.newInt(1))
        struct.add(intList[1].toString(), ION.newInt(2))
        struct.add(intList[2].toString(), ION.newInt(3))

        return struct
    }

    private fun makeBigintIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val bigintList = listOf<Long>(1L, 2L, 3L)

        struct.add(bigintList[0].toString(), ION.newInt(1))
        struct.add(bigintList[1].toString(), ION.newInt(2))
        struct.add(bigintList[2].toString(), ION.newInt(3))

        return struct
    }

    private fun makeSmallintIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val smallintList = listOf<Short>(1, 2, 3)

        struct.add(smallintList[0].toString(), ION.newInt(1))
        struct.add(smallintList[1].toString(), ION.newInt(2))
        struct.add(smallintList[2].toString(), ION.newInt(3))

        return struct
    }

    private fun makeTinyintIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val smallintList = listOf<Byte>(1, 2, 3)

        struct.add(smallintList[0].toString(), ION.newInt(1))
        struct.add(smallintList[1].toString(), ION.newInt(2))
        struct.add(smallintList[2].toString(), ION.newInt(3))

        return struct
    }

    private fun makeFloatIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val floatList = listOf<Float>(2f, 4f, 8f)

        struct.add(floatList[0].toString(), ION.newInt(1))
        struct.add(floatList[1].toString(), ION.newInt(2))
        struct.add(floatList[2].toString(), ION.newInt(3))

        return struct
    }

    private fun makeDoubleIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val doubleList = listOf<Double>(2.0, 4.0, 6.0)

        struct.add(doubleList[0].toString(), ION.newInt(1))
        struct.add(doubleList[1].toString(), ION.newInt(2))
        struct.add(doubleList[2].toString(), ION.newInt(3))

        return struct
    }

    private fun makeBoolIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val boolList = listOf<Boolean>(true, false)

        struct.add(boolList[0].toString(), ION.newInt(1))
        struct.add(boolList[1].toString(), ION.newInt(2))

        return struct
    }

    private fun makeDecimalIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val decimalList = listOf<HiveDecimal>(HiveDecimal.create(1), HiveDecimal.create(2))

        struct.add(decimalList[0].toString(), ION.newInt(1))
        struct.add(decimalList[1].toString(), ION.newInt(2))

        return struct
    }

    private fun makeDateIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val dateList = listOf<Date>(
                Date.valueOf("1991-1-1"),
                Date.valueOf("1992-2-3"))

        struct.add(dateList[0].toString(), ION.newInt(1))
        struct.add(dateList[1].toString(), ION.newInt(2))

        return struct
    }

    private fun makeTimestampIntStruct(): IonStruct {
        val struct = ION.newEmptyStruct()
        val tsList = listOf<Timestamp>(
                Timestamp.valueOf("2014-10-14 12:34:56.789"),
                Timestamp.valueOf("2015-10-16 12:34:56.789"))

        struct.add(tsList[0].toString(), ION.newInt(1))
        struct.add(tsList[1].toString(), ION.newInt(2))

        return struct
    }

}