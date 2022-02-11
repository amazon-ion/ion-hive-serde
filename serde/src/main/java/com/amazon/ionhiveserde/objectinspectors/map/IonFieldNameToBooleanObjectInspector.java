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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;

public class IonFieldNameToBooleanObjectInspector
        extends AbstractOverflowableFieldNameObjectInspector<String, Boolean>
        implements BooleanObjectInspector {

    public IonFieldNameToBooleanObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.booleanTypeInfo, failOnOverflow);
    }

    @Override
    protected Boolean getValidatedPrimitiveJavaObject(final String fieldName) {
        return Boolean.parseBoolean(fieldName);
    }

    @Override
    protected void validateSize(final String fieldName) {
        try {
            Boolean booleanValue = Boolean.parseBoolean(fieldName);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "invalid format for " + fieldName + " as " + this.typeInfo.getTypeName());
        }
    }

    @Override
    public boolean get(final Object o) {
        return (boolean) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        return getPrimitiveJavaObjectFromIonValue(o.toString());
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        return new BooleanWritable(getPrimitiveJavaObjectFromIonValue(o.toString()));
    }
}