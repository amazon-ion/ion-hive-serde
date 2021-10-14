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

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.system.IonTextWriterBuilder;

import java.util.Iterator;

import org.jetbrains.annotations.NotNull;

class IonContainerCaseInsensitiveDecorator implements IonContainer {
    IonContainer ionContainer;

    public IonContainerCaseInsensitiveDecorator(final IonContainer c) {
        this.ionContainer = c;
    }

    @Override
    public int size() {
        return ionContainer.size();
    }

    @NotNull
    @Override
    public Iterator<IonValue> iterator() {
        return new IteratorCaseInsensitiveDecorator(ionContainer.iterator());
    }

    @Override
    public boolean remove(final IonValue element) {
        return ionContainer.remove(element);
    }

    @Override
    public boolean isEmpty() throws NullValueException {
        return ionContainer.isEmpty();
    }

    @Override
    public void clear() {
        ionContainer.clear();
    }

    @Override
    public void makeNull() {
        ionContainer.makeNull();
    }

    @Override
    public IonContainer clone() throws UnknownSymbolException {
        return new IonContainerCaseInsensitiveDecorator(ionContainer.clone());
    }

    @Override
    public IonType getType() {
        return ionContainer.getType();
    }

    @Override
    public boolean isNullValue() {
        return ionContainer.isNullValue();
    }

    @Override
    public boolean isReadOnly() {
        return ionContainer.isReadOnly();
    }

    @Override
    public SymbolTable getSymbolTable() {
        return ionContainer.getSymbolTable();
    }

    @Override
    public String getFieldName() {
        return ionContainer.getFieldName();
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        return ionContainer.getFieldNameSymbol();
    }

    @Override
    @Deprecated
    public int getFieldId() {
        return ionContainer.getFieldId();
    }

    @Override
    public IonContainer getContainer() {
        return new IonContainerCaseInsensitiveDecorator(ionContainer.getContainer());
    }

    @Override
    public boolean removeFromContainer() {
        return ionContainer.removeFromContainer();
    }

    @Override
    public IonValue topLevelValue() {
        return IonCaseInsensitiveDecorator.wrapValue(ionContainer.topLevelValue());
    }

    @Override
    public String[] getTypeAnnotations() {
        return ionContainer.getTypeAnnotations();
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        return ionContainer.getTypeAnnotationSymbols();
    }

    @Override
    public boolean hasTypeAnnotation(final String annotation) {
        return ionContainer.hasTypeAnnotation(annotation);
    }

    @Override
    public void setTypeAnnotations(final String... annotations) {
        ionContainer.setTypeAnnotations(annotations);
    }

    @Override
    public void setTypeAnnotationSymbols(final SymbolToken... annotations) {
        ionContainer.setTypeAnnotationSymbols(annotations);
    }

    @Override
    public void clearTypeAnnotations() {
        ionContainer.clearTypeAnnotations();
    }

    @Override
    public void addTypeAnnotation(final String annotation) {
        ionContainer.addTypeAnnotation(annotation);
    }

    @Override
    public void removeTypeAnnotation(final String annotation) {
        ionContainer.removeTypeAnnotation(annotation);
    }

    @Override
    public void writeTo(final IonWriter writer) {
        ionContainer.writeTo(writer);
    }

    @Override
    public void accept(final ValueVisitor visitor) throws Exception {
        ionContainer.accept(visitor);
    }

    @Override
    public void makeReadOnly() {
        ionContainer.makeReadOnly();
    }

    @Override
    public IonSystem getSystem() {
        return ionContainer.getSystem();
    }

    @Override
    public String toPrettyString() {
        return ionContainer.toPrettyString();
    }

    @Override
    public String toString(final IonTextWriterBuilder writerBuilder) {
        return ionContainer.toString();
    }

    public class IteratorCaseInsensitiveDecorator implements Iterator<IonValue> {
        Iterator<IonValue> iterator;

        public IteratorCaseInsensitiveDecorator(final Iterator<IonValue> l) {
            this.iterator = l;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public IonValue next() {
            return IonCaseInsensitiveDecorator.wrapValue(iterator.next());
        }
    }
}
