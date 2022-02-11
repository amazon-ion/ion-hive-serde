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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.DoubleWritable;

public class IonFieldNameToDoubleObjectInspector
        extends AbstractFieldNameObjectInspector<Double>
        implements DoubleObjectInspector {

    public IonFieldNameToDoubleObjectInspector() {
        super(TypeInfoFactory.doubleTypeInfo);
    }

    @Override
    public double get(final Object o) {
        return (double) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        return getPrimitiveJavaObjectFromFieldName(o);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        return new DoubleWritable(getPrimitiveJavaObjectFromFieldName(o));
    }

    @Override
    protected Double getValidatedPrimitiveJavaObject(final String fieldName) {
        try {
            return IonPrimitiveReader.doubleValue(fieldName);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid format for " + fieldName + " as " + this.typeInfo.getTypeName());
        }
    }
}