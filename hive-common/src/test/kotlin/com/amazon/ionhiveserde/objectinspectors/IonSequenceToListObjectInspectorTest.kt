// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonSequence
import com.amazon.ionhiveserde.ION
import com.amazon.ionhiveserde.ionNull
import junitparams.JUnitParamsRunner
import junitparams.NamedParameters
import junitparams.Parameters
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals
import kotlin.test.assertNull

@RunWith(JUnitParamsRunner::class)
class IonSequenceToListObjectInspectorTest {

    private val elementObjectInspector = com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector(true)
    private val subject = com.amazon.ionhiveserde.objectinspectors.IonSequenceToListObjectInspector(elementObjectInspector)

    @Test
    fun getListElementObjectInspector() {
        assertEquals(elementObjectInspector, subject.listElementObjectInspector)
    }

    @NamedParameters("sequences")
    fun sequences(): Array<IonSequence> =
            arrayOf(ION.newEmptyList(), ION.newEmptySexp()).map {
                it.add(ION.newInt(1))
                it.add(ION.newInt(2))
                it.add(ION.newNullInt())
                it
            }.toTypedArray()

    @Test
    @Parameters(named = "sequences")
    fun getListElement(sequence: IonSequence) {
        assertEquals(ION.newInt(1), subject.getListElement(sequence, 0))
        assertEquals(ION.newInt(2), subject.getListElement(sequence, 1))
        assertEquals(null, subject.getListElement(sequence, 2))

        assertNull(subject.getListElement(sequence, 4))
        assertNull(subject.getListElement(sequence, -1))
    }

    @Test
    fun getListElementForNull() {
        assertNull(subject.getListElement(null, 0))
        assertNull(subject.getListElement(ionNull, 0))
    }

    @Test
    @Parameters(named = "sequences")
    fun getListLength(sequence: IonSequence) {
        assertEquals(3, subject.getListLength(sequence))
    }

    @Test
    fun getListLengthNullData() {
        assertEquals(-1, subject.getListLength(null))
    }

    @Test
    @Parameters(named = "sequences")
    fun getList(sequence: IonSequence) {
        val actual = subject.getList(sequence)
        // We should make sure the null value is inside the list returned by getList()
        assertEquals(3, actual.size)
        assertEquals(ION.newInt(1), actual[0])
        assertEquals(ION.newInt(2), actual[1])
        assertEquals(null, actual[2])
    }

    @Test
    fun getListNull() {
        assertNull(subject.getList(null))
    }

    @Test
    fun getCategory() {
        assertEquals(LIST, subject.category)
    }

    @Test
    fun getTypeName() {
        assertEquals("array<int>", subject.typeName)
    }
}
