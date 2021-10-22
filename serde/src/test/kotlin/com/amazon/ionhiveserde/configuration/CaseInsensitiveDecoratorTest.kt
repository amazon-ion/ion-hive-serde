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

package com.amazon.ionhiveserde.configuration

import com.amazon.ion.*
import com.amazon.ionhiveserde.ION
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonSequenceCaseInsensitiveDecorator
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonStructCaseInsensitiveDecorator
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CaseInsensitiveDecoratorTest {
    private fun datagram_for(s: String): IonDatagram {
        return ION.loader.load(s)
    }

    private fun struct_for(s: String): IonStruct {
        val v = datagram_for(s).iterator().next()
        if (v !is IonStruct) throw IllegalArgumentException("Required an IonStruct, found ${v.javaClass.simpleName}")

        return IonStructCaseInsensitiveDecorator(v)
    }

    private fun sequence_for(s: String): IonSequence {
        val v = datagram_for(s).iterator().next()
        if (v !is IonSequence) throw IllegalArgumentException("Required an IonSequence, found ${v.javaClass.simpleName}")

        return IonSequenceCaseInsensitiveDecorator(v)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorContainsKey() {
        val struct = struct_for(" { Foo: 'bar' }")
        assertEquals(struct.containsKey("foo"), true)
        assertEquals(struct.containsKey("bar"), false)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetExist() {
        val struct = struct_for(" { Foo: 'bar' }")
        assertEquals(struct.containsKey("Foo"), true)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetNotExist() {
        val struct = struct_for(" { Foo: 'bar' }")
        assertEquals(struct.containsKey("bar"), false)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetIgnoreCase() {
        val struct = struct_for(" { Foo: 'bar' }")
        assertEquals(struct.containsKey("foO"), true)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetRepeatedFieldFound() {
        val struct = struct_for(" { Foo: 'Bar', foo: 'bar' }")
        assertEquals(struct.get("Foo"), ION.newSymbol("Bar"))
        assertEquals(struct.get("foo"), ION.newSymbol("bar"))
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetRepeatedFieldFoundIgnoringCase() {
        val struct = struct_for(" { Foo: 'Bar', foo: 'bar' }")
        assert(
                struct.get("FOO") == ION.newSymbol("Bar") || struct.get("FOO") == ION.newSymbol("bar")
        )
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetStruct() {
        val struct = struct_for(" { Foo: {} }")
        assertTrue(struct.get("Foo") is IonStructCaseInsensitiveDecorator, "Found ${struct.get("Foo").javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetSequence() {
        val struct = struct_for(" { Foo: [] }")
        assertTrue(struct.get("Foo") is IonSequenceCaseInsensitiveDecorator, "Found ${struct.get("Foo").javaClass.simpleName} ,expected IonSequenceCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorRemove() {
        val struct = struct_for(" { Foo: 'bar' }")
        val s = struct.remove("Foo")

        assertEquals(0, struct.size())
        assertEquals(s, ION.newSymbol("bar"))
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorRemoveStruct() {
        val struct = struct_for(" { Foo: {} }")
        val s = struct.remove("Foo")
        assertTrue(s is IonStructCaseInsensitiveDecorator, "Found ${s.javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorRemoveSequence() {
        val struct = struct_for(" { Foo: [] }")
        val s = struct.remove("Foo")
        assertTrue(s is IonSequenceCaseInsensitiveDecorator, "Found ${s.javaClass.simpleName} ,expected IonSequenceCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorCloneAndRemove() {
        val struct = struct_for(" { Foo: 'bar' }")
        val s = struct.cloneAndRemove("Foo")

        assertEquals(s.size(), 0)
        assertTrue(s is IonStructCaseInsensitiveDecorator, "Found ${s.javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorCloneAndRetain() {
        val struct = struct_for(" { Foo: 'bar' }")
        val s = struct.cloneAndRetain("Foo")

        assertEquals(s.size(), 1)
        assertEquals(s.containsKey("foo"), true)
        assertTrue(s is IonStructCaseInsensitiveDecorator, "Found ${s.javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorGet() {
        val sequence = sequence_for("[1, '2']")
        assertEquals(sequence[1], ION.newSymbol("2"))
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorGetStruct() {
        val sequence = sequence_for("[{}]")
        assertTrue(sequence[0] is IonStructCaseInsensitiveDecorator, "Found ${sequence[0].javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorGetSequence() {
        val sequence = sequence_for("[[]]")
        assertTrue(sequence[0] is IonSequenceCaseInsensitiveDecorator, "Found ${sequence[0].javaClass.simpleName} ,expected IonSequenceCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorSet() {
        val sequence = sequence_for("[1]")
        val l = sequence.set(0, ION.newInt(2))

        assertEquals(sequence[0], ION.newInt(2))
        assertEquals(l, ION.newInt(1))
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorSetStruct() {
        val sequence = sequence_for("[{}]")
        val l = sequence.set(0, ION.newInt(2))
        assertTrue(l is IonStructCaseInsensitiveDecorator, "Found ${l.javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorSetSequence() {
        val sequence = sequence_for("[[]]")
        val l = sequence.set(0, ION.newInt(2))
        assertTrue(l is IonSequenceCaseInsensitiveDecorator, "Found ${l.javaClass.simpleName} ,expected IonSequenceCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorRemove() {
        val sequence = sequence_for("[1]")
        val l = sequence.removeAt(0)

        assertEquals(sequence.size, 0)
        assertEquals(l, ION.newInt(1))
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorRemoveStruct() {
        val sequence = sequence_for("[{}]")
        val l = sequence.removeAt(0)

        assertTrue(l is IonStructCaseInsensitiveDecorator, "Found ${l.javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorRemoveSequence() {
        val sequence = sequence_for("[[]]")
        val l = sequence.removeAt(0)

        assertTrue(l is IonSequenceCaseInsensitiveDecorator, "Found ${l.javaClass.simpleName} ,expected IonSequenceCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorListIterator() {
        val sequence = sequence_for("[1, [], {}]")
        val iter = sequence.listIterator()
        assertEquals(iter.next(), ION.newInt(1))
        val i = iter.next()
        assertTrue(i is IonSequenceCaseInsensitiveDecorator, "Found ${i.javaClass.simpleName} ,expected IonSequenceCaseInsensitiveDecorator type.")
        val ii = iter.next()
        assertTrue(ii is IonStructCaseInsensitiveDecorator, "Found ${ii.javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorSublist() {
        val sequence = sequence_for("[1, [], {}]")
        val sublist = sequence.subList(0, 3)
        // sublist: [1, [], {}]
        assertEquals(sublist[0], ION.newInt(1))
        assertTrue(sublist[1] is IonSequenceCaseInsensitiveDecorator, "Found ${sublist[1].javaClass.simpleName} ,expected IonSequenceCaseInsensitiveDecorator type.")
        assertTrue(sublist[2] is IonStructCaseInsensitiveDecorator, "Found ${sublist[2].javaClass.simpleName} ,expected IonStructCaseInsensitiveDecorator type.")
    }
}
