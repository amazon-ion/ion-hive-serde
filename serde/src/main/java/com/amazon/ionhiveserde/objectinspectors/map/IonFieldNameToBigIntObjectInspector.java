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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

public class IonFieldNameToBigIntObjectInspector
        extends AbstractOverflowableFieldNameObjectInspector<String, Long>
        implements LongObjectInspector {

    public static final long MIN_VALUE = Long.MIN_VALUE;
    public static final long MAX_VALUE = Long.MAX_VALUE;

    public IonFieldNameToBigIntObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.longTypeInfo, failOnOverflow);
    }

    @Override
    protected Long getValidatedPrimitiveJavaObject(final String fieldName) {
        return Long.parseLong(fieldName);
    }

    @Override
    protected void validateSize(final String fieldName) {
        try {
            long value = Long.parseLong(fieldName);

            if (!validRange(value)) {
                throw new IllegalArgumentException(
                        "insufficient precision for " + value + " as " + this.typeInfo.getTypeName());
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "invalid format for " + fieldName + " as " + this.typeInfo.getTypeName());
        }
    }

    private boolean validRange(final long value) {
        // runs after checking that fits in a Java long
        return MIN_VALUE <= value && value <= MAX_VALUE;
    }

    @Override
    public long get(final Object o) {
        return (long) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        return getPrimitiveJavaObjectFromIonValue(o.toString());
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        return new LongWritable(getPrimitiveJavaObjectFromIonValue(o.toString()));
    }
}