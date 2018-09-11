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

import junitparams.JUnitParamsRunner
import junitparams.NamedParameters
import junitparams.Parameters
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonSequence
import software.amazon.ionhiveserde.ION
import software.amazon.ionhiveserde.ionNull
import kotlin.test.assertEquals
import kotlin.test.assertNull

@RunWith(JUnitParamsRunner::class)
class IonSequenceToListObjectInspectorTest {

    private val elementObjectInspector = IonIntToIntObjectInspector()
    private val subject = IonSequenceToListObjectInspector(elementObjectInspector)

    @Test
    fun getListElementObjectInspector() {
        assertEquals(elementObjectInspector, subject.listElementObjectInspector)
    }

    @NamedParameters("sequences")
    fun sequences(): Array<IonSequence> =
            arrayOf(ION.newEmptyList(), ION.newEmptySexp()).map {
                it.add(ION.newInt(1))
                it.add(ION.newInt(2))
                it.add(ION.newInt(3))
                it
            }.toTypedArray()

    @Test
    @Parameters(named = "sequences")
    fun getListElement(sequence: IonSequence) {
        assertEquals(ION.newInt(1), subject.getListElement(sequence, 0))
        assertEquals(ION.newInt(2), subject.getListElement(sequence, 1))
        assertEquals(ION.newInt(3), subject.getListElement(sequence, 2))

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
        assertEquals(sequence, actual)
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