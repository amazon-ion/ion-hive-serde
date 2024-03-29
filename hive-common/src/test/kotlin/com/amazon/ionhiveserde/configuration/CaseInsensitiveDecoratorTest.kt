package com.amazon.ionhiveserde.configuration

import com.amazon.ion.IonValue
import com.amazon.ionhiveserde.ION
import com.amazon.ionhiveserde.assertMultiEquals
import com.amazon.ionhiveserde.assertSequenceWrapper
import com.amazon.ionhiveserde.assertStructWrapper
import com.amazon.ionhiveserde.case_insensitive
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonSequenceCaseInsensitiveDecorator
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonStructCaseInsensitiveDecorator
import com.amazon.ionhiveserde.sequence_for
import com.amazon.ionhiveserde.struct_for
import org.junit.Test
import kotlin.test.assertEquals

class CaseInsensitiveDecoratorTest {
    @Test
    fun ionStructCaseInsensitiveDecoratorContainsKey() {
        val struct = case_insensitive(struct_for("{Foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        assertEquals(struct.containsKey("foo"), true)
        assertEquals(struct.containsKey("bar"), false)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetExist() {
        val struct = case_insensitive(struct_for("{Foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        assertEquals(struct.containsKey("Foo"), true)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetNotExist() {
        val struct = case_insensitive(struct_for("{Foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        assertEquals(struct.containsKey("bar"), false)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetIgnoreCase() {
        val struct = case_insensitive(struct_for("{Foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        assertEquals(struct.containsKey("foO"), true)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetRepeatedFieldFound() {
        val struct = case_insensitive(struct_for("{Foo: 'Bar', foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        assertEquals(struct.get("Foo"), ION.newSymbol("Bar"))
        assertEquals(struct.get("foo"), ION.newSymbol("bar"))
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetRepeatedFieldFoundIgnoringCase() {
        val struct = case_insensitive(struct_for("{Foo: 'Bar', foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        assertMultiEquals(
            arrayOf(ION.newSymbol("Bar") as IonValue, ION.newSymbol("bar") as IonValue),
            struct.get("FOO")
        )
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetStruct() {
        val struct = case_insensitive(struct_for("{Foo: {}}")) as IonStructCaseInsensitiveDecorator
        assertStructWrapper(struct.get("Foo"))
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorGetSequence() {
        val struct = case_insensitive(struct_for("{ Foo: []}")) as IonStructCaseInsensitiveDecorator
        assertSequenceWrapper(struct.get("Foo"))
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorRemove() {
        val struct = case_insensitive(struct_for("{Foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        val s = struct.remove("Foo")

        assertEquals(0, struct.size())
        assertEquals(s, ION.newSymbol("bar"))
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorRemoveStruct() {
        val struct = case_insensitive(struct_for("{Foo: {}}")) as IonStructCaseInsensitiveDecorator
        val s = struct.remove("Foo")
        assertStructWrapper(s)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorRemoveSequence() {
        val struct = case_insensitive(struct_for("{Foo: []}")) as IonStructCaseInsensitiveDecorator
        val s = struct.remove("Foo")
        assertSequenceWrapper(s)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorCloneAndRemove() {
        val struct = case_insensitive(struct_for("{Foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        val s = struct.cloneAndRemove("Foo")

        assertEquals(s.size(), 0)
        assertStructWrapper(s)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorCloneAndRetain() {
        val struct = case_insensitive(struct_for("{Foo: 'bar'}")) as IonStructCaseInsensitiveDecorator
        val s = struct.cloneAndRetain("Foo")

        assertEquals(s.size(), 1)
        assertEquals(s.containsKey("foo"), true)
        assertStructWrapper(s)
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorIteratorNext() {
        val struct = case_insensitive(struct_for("{Foo: 1, Bar: '2'}")) as IonStructCaseInsensitiveDecorator
        val iter = struct.iterator()
        val foo = iter.next()
        assertEquals(foo, ION.newInt(1))
        val bar = iter.next()
        assertEquals(bar, ION.newSymbol("2"))
    }

    @Test
    fun ionStructCaseInsensitiveDecoratorIteratorNull() {
        val struct = case_insensitive(struct_for("{Foo: null}")) as IonStructCaseInsensitiveDecorator
        val iter = struct.iterator()
        val v = iter.next()
        // Returns Ion null like what Ion container does
        assertEquals(v, ION.newNull())
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorGet() {
        val sequence = case_insensitive(sequence_for("[1, '2']")) as IonSequenceCaseInsensitiveDecorator
        assertEquals(sequence[1], ION.newSymbol("2"))
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorGetStruct() {
        val sequence = case_insensitive(sequence_for("[{}]")) as IonSequenceCaseInsensitiveDecorator
        assertStructWrapper(sequence[0])
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorGetSequence() {
        val sequence = case_insensitive(sequence_for("[[]]")) as IonSequenceCaseInsensitiveDecorator
        assertSequenceWrapper(sequence[0])
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorSet() {
        val sequence = case_insensitive(sequence_for("[1]")) as IonSequenceCaseInsensitiveDecorator
        val l = sequence.set(0, ION.newInt(2))

        assertEquals(sequence[0], ION.newInt(2))
        assertEquals(l, ION.newInt(1))
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorSetStruct() {
        val sequence = case_insensitive(sequence_for("[{}]")) as IonSequenceCaseInsensitiveDecorator
        val l = sequence.set(0, ION.newInt(2))
        assertStructWrapper(l)
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorSetSequence() {
        val sequence = case_insensitive(sequence_for("[[]]")) as IonSequenceCaseInsensitiveDecorator
        val l = sequence.set(0, ION.newInt(2))
        assertSequenceWrapper(l)
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorRemove() {
        val sequence = case_insensitive(sequence_for("[1]")) as IonSequenceCaseInsensitiveDecorator
        val l = sequence.removeAt(0)

        assertEquals(sequence.size, 0)
        assertEquals(l, ION.newInt(1))
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorRemoveStruct() {
        val sequence = case_insensitive(sequence_for("[{}]")) as IonSequenceCaseInsensitiveDecorator
        val l = sequence.removeAt(0)

        assertStructWrapper(l)
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorRemoveSequence() {
        val sequence = case_insensitive(sequence_for("[[]]")) as IonSequenceCaseInsensitiveDecorator
        val l = sequence.removeAt(0)

        assertSequenceWrapper(l)
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorListIterator() {
        val sequence = case_insensitive(sequence_for("[1, [], {}]")) as IonSequenceCaseInsensitiveDecorator
        val iter = sequence.listIterator()
        assertEquals(iter.next(), ION.newInt(1))
        val i = iter.next()
        assertSequenceWrapper(i)
        val ii = iter.next()
        assertStructWrapper(ii)
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorSublist() {
        val sequence = case_insensitive(sequence_for("[1, [], {}]")) as IonSequenceCaseInsensitiveDecorator
        val sublist = sequence.subList(0, 3)
        // sublist: [1, [], {}]
        assertEquals(sublist[0], ION.newInt(1))
        assertSequenceWrapper(sublist[1])
        assertStructWrapper(sublist[2])
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorIteratorNext() {
        val sequence = case_insensitive(sequence_for("[1, '2']")) as IonSequenceCaseInsensitiveDecorator
        val iter = sequence.iterator()
        assertEquals(iter.next(), ION.newInt(1))
        assertEquals(iter.next(), ION.newSymbol("2"))
    }

    @Test
    fun ionSequenceCaseInsensitiveDecoratorIteratorNull() {
        val sequence = case_insensitive(sequence_for("[null]")) as IonSequenceCaseInsensitiveDecorator
        val iter = sequence.iterator()
        val v = iter.next()
        // Returns Ion null like what Ion container does
        assertEquals(v, ION.newNull());
    }
}
