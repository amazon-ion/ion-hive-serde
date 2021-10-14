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

package com.amazon.ionhiveserde.caseinsensitivedecorator;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonValue;
import com.amazon.ion.NullValueException;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import org.jetbrains.annotations.NotNull;




public class IonSequenceCaseInsensitiveDecorator extends IonContainerCaseInsensitiveDecorator implements IonSequence {
    private final IonSequence ionSequence;

    public IonSequenceCaseInsensitiveDecorator(final IonSequence s) {
        super(s);
        this.ionSequence = s;
    }

    @Override
    public IonValue get(final int index) throws NullValueException, IndexOutOfBoundsException {
        IonValue v = ionSequence.get(index);
        return IonCaseInsensitiveDecorator.wrapValue(v);
    }

    @Override
    public boolean add(final IonValue child) throws ContainedValueException, NullPointerException {
        return ionSequence.add(child);
    }

    @Override
    public ValueFactory add() {
        return ionSequence.add();
    }

    @Override
    public void add(final int index, final IonValue child) throws ContainedValueException, NullPointerException {
        ionSequence.add();
    }

    @Override
    public ValueFactory add(final int index) {
        return ionSequence.add(index);
    }

    @Override
    public IonValue set(final int index, final IonValue element) {
        return IonCaseInsensitiveDecorator.wrapValue(ionSequence.set(index, element));
    }

    @Override
    public IonValue remove(final int index) {
        return IonCaseInsensitiveDecorator.wrapValue(ionSequence.remove(index));
    }

    @Override
    public boolean remove(final Object o) {
        return ionSequence.remove(o);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        return ionSequence.removeAll(c);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        return ionSequence.retainAll(c);
    }

    @Override
    public boolean contains(final Object o) {
        return ionSequence.contains(o);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return ionSequence.containsAll(c);
    }

    @Override
    public int indexOf(final Object o) {
        return ionSequence.indexOf(o);
    }

    @Override
    public int lastIndexOf(final Object o) {
        return ionSequence.lastIndexOf(o);
    }

    @Override
    public boolean addAll(final Collection<? extends IonValue> c) {
        return ionSequence.addAll(c);
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends IonValue> c) {
        return ionSequence.addAll(index, c);
    }

    @NotNull
    @Override
    public ListIterator<IonValue> listIterator() {
        return new ListIteratorCaseInsensitiveDecorator(ionSequence.listIterator());
    }

    @NotNull
    @Override
    public ListIterator<IonValue> listIterator(final int index) {
        return new ListIteratorCaseInsensitiveDecorator(ionSequence.listIterator(index));
    }

    @NotNull
    @Override
    public List<IonValue> subList(final int fromIndex, final int toIndex) {
        List<IonValue> l = new ArrayList<>();
        List<IonValue> sublist = ionSequence.subList(fromIndex, toIndex);

        for (IonValue ionValue : sublist) {
            l.add(IonCaseInsensitiveDecorator.wrapValue(ionValue));
        }
        return l;
    }

    @NotNull
    @Override
    public IonValue[] toArray() {
        IonValue[] rawArray = ionSequence.toArray();
        int size = ionSequence.size();
        for (int i = 0; i < size; i++) {
            rawArray[i] = IonCaseInsensitiveDecorator.wrapValue(rawArray[i]);
        }
        return rawArray;
    }

    @NotNull
    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(final T[] a) {
        T[] rawArray = ionSequence.toArray(a);
        int size = rawArray.length;
        for (int i = 0; i < size; i++) {
            rawArray[i] = (T) IonCaseInsensitiveDecorator.wrapValue((IonValue) rawArray[i]);
        }
        return rawArray;
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T extends IonValue> T[] extract(final Class<T> type) {
        T[] rawArray = ionSequence.extract(type);
        int size = rawArray.length;
        for (int i = 0; i < size; i++) {
            rawArray[i] = (T) IonCaseInsensitiveDecorator.wrapValue((IonValue) rawArray[i]);
        }
        return rawArray;
    }

    public IonSequence clone() throws UnknownSymbolException {
        return new IonSequenceCaseInsensitiveDecorator(ionSequence.clone());
    }

    public static class ListIteratorCaseInsensitiveDecorator implements ListIterator<IonValue> {
        ListIterator<IonValue> listIterator;

        public ListIteratorCaseInsensitiveDecorator(final ListIterator<IonValue> l) {
            this.listIterator = l;
        }

        @Override
        public boolean hasNext() {
            return listIterator.hasNext();
        }

        @Override
        public IonValue next() {
            return IonCaseInsensitiveDecorator.wrapValue(listIterator.next());
        }

        @Override
        public boolean hasPrevious() {
            return listIterator.hasPrevious();
        }

        @Override
        public IonValue previous() {
            return IonCaseInsensitiveDecorator.wrapValue(listIterator.previous());
        }

        @Override
        public int nextIndex() {
            return listIterator.nextIndex();
        }

        @Override
        public int previousIndex() {
            return listIterator.previousIndex();
        }

        @Override
        public void remove() {
            listIterator.remove();
        }

        @Override
        public void set(final IonValue ionValue) {
            listIterator.set(ionValue);
        }

        @Override
        public void add(final IonValue ionValue) {
            listIterator.add(ionValue);
        }
    }
}
