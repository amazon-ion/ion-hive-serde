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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;

public class IonFieldNameToFloatObjectInspector
        extends AbstractOverflowableFieldNameObjectInspector<String, Float>
        implements FloatObjectInspector {

    public IonFieldNameToFloatObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.floatTypeInfo, failOnOverflow);
    }

    @Override
    protected Float getValidatedPrimitiveJavaObject(final String fieldName) {
        return Float.parseFloat(fieldName);
    }

    @Override
    protected void validateSize(final String fieldName) {
        try {
            double doubleValue = Double.parseDouble(fieldName);
            double floatValue = Float.parseFloat(fieldName);

            if (Double.compare(floatValue, doubleValue) != 0) {
                throw new IllegalArgumentException(
                        "insufficient precision for " + fieldName + " as " + this.typeInfo.getTypeName());
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "invalid format for " + fieldName + " as " + this.typeInfo.getTypeName());
        }
    }

    @Override
    public float get(final Object o) {
        return (float) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        return getPrimitiveJavaObjectFromIonValue(o.toString());
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        return new FloatWritable(getPrimitiveJavaObjectFromIonValue(o.toString()));
    }
}