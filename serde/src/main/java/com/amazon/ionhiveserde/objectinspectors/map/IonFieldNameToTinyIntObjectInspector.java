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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ShortWritable;

public class IonFieldNameToTinyIntObjectInspector
        extends AbstractFieldNameObjectInspector<Byte>
        implements ByteObjectInspector {

    public IonFieldNameToTinyIntObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.byteTypeInfo, failOnOverflow);
    }

    @Override
    public byte get(final Object o) {
        return (byte) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        return new ShortWritable(getPrimitiveJavaObjectFromFieldName(o));
    }

    @Override
    protected Byte getValidatedPrimitiveJavaObject(final String fieldName) {
        try {
            return IonPrimitiveReader.byteValue(fieldName);
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
        // runs after checking that fits in a Java byte
        return Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE;
    }
}