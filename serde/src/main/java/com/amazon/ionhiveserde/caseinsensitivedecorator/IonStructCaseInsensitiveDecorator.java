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
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueFactory;

import java.util.Map;


public class IonStructCaseInsensitiveDecorator extends IonContainerCaseInsensitiveDecorator implements IonStruct {
    private final IonStruct ionStruct;

    public IonStructCaseInsensitiveDecorator(final IonStruct s) {
        super(s);
        this.ionStruct = s;
    }

    @Override
    public boolean containsKey(final Object fieldName) {
        return ionStruct.containsKey(fieldName) || hasCaseInsensitiveFieldMatch(fieldName);
    }

    @Override
    public boolean containsValue(final Object value) {
        return ionStruct.containsValue(value);
    }

    @Override
    public IonValue get(final String fieldName) {
        IonValue v = ionStruct.get(fieldName);

        if (v == null) {
            String match = findCaseInsensitiveFieldMatch(fieldName);
            if (match != null) {
                return IonCaseInsensitiveDecorator.wrapValue(ionStruct.get(match));
            }
        }

        return IonCaseInsensitiveDecorator.wrapValue(v);
    }

    @Override
    public void put(final String fieldName, final IonValue child) throws ContainedValueException {
        ionStruct.put(fieldName, child);
    }

    @Override
    public ValueFactory put(final String fieldName) {
        return ionStruct.put(fieldName);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends IonValue> m) {
        ionStruct.putAll(m);
    }

    @Override
    public void add(final String fieldName, final IonValue child) throws ContainedValueException {
        ionStruct.add(fieldName, child);
    }

    @Override
    public void add(final SymbolToken fieldName, final IonValue child) throws ContainedValueException {
        ionStruct.add(fieldName, child);
    }

    @Override
    public ValueFactory add(final String fieldName) {
        return ionStruct.add(fieldName);
    }

    @Override
    public IonValue remove(final String fieldName) {
        return IonCaseInsensitiveDecorator.wrapValue(ionStruct.remove(fieldName));
    }

    @Override
    public boolean removeAll(final String... fieldNames) {
        return ionStruct.removeAll(fieldNames);
    }

    @Override
    public boolean retainAll(final String... fieldNames) {
        return ionStruct.retainAll(fieldNames);
    }

    @Override
    public IonStruct cloneAndRemove(final String... fieldNames) throws UnknownSymbolException {
        return new IonStructCaseInsensitiveDecorator(ionStruct.cloneAndRemove(fieldNames));
    }

    @Override
    public IonStruct cloneAndRetain(final String... fieldNames) throws UnknownSymbolException {
        return new IonStructCaseInsensitiveDecorator(ionStruct.cloneAndRetain(fieldNames));
    }

    public IonStruct clone() throws UnknownSymbolException {
        return new IonStructCaseInsensitiveDecorator(ionStruct.clone());
    }

    private String findCaseInsensitiveFieldMatch(final String fieldName) {
        for (IonValue v : ionStruct) {
            if (fieldName.equalsIgnoreCase(v.getFieldName())) {
                return v.getFieldName();
            }
        }
        return null;
    }

    private boolean hasCaseInsensitiveFieldMatch(final Object fieldName) {
        if (fieldName instanceof String) {
            return findCaseInsensitiveFieldMatch((String)fieldName) != null;
        } else {
            return false;
        }
    }
}
