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

package com.amazon.ionhiveserde.objectinspectors.map;

import com.amazon.ionhiveserde.objectinspectors.utils.IonPrimitiveReader;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ShortWritable;

public class IonFieldNameToSmallIntObjectInspector
        extends AbstractFieldNameObjectInspector<Short>
        implements ShortObjectInspector {
    public static final long MIN_VALUE = Short.MIN_VALUE;
    public static final long MAX_VALUE = Short.MAX_VALUE;

    public IonFieldNameToSmallIntObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.shortTypeInfo, failOnOverflow);
    }

    @Override
    public short get(final Object o) {
        return (short) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        return getPrimitiveJavaObjectFromFieldName(o.toString());
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        return new ShortWritable(getPrimitiveJavaObjectFromFieldName(o.toString()));
    }

    @Override
    protected Short getValidatedPrimitiveJavaObject(final String fieldName) {
        try {
            return IonPrimitiveReader.shortValue(fieldName);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid format for " + fieldName + " as " + this.typeInfo.getTypeName());
        }
    }

    @Override
    protected void validateSize(final String fieldName) {
        long value = IonPrimitiveReader.longValue(fieldName);

        if (!validRange(value)) {
            throw new IllegalArgumentException(
                    "insufficient precision for " + value + " as " + this.typeInfo.getTypeName());
        }
    }

    private boolean validRange(final long value) {
        // runs after checking that fits in a Java short
        return MIN_VALUE <= value && value <= MAX_VALUE;
    }
}