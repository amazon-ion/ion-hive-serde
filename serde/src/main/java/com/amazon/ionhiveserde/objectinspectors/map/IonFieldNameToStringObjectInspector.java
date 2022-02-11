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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

public class IonFieldNameToStringObjectInspector
        extends AbstractOverflowableFieldNameObjectInspector<String, String>
        implements StringObjectInspector {

    public IonFieldNameToStringObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.stringTypeInfo, failOnOverflow);
    }

    @Override
    public Text getPrimitiveWritableObject(final Object o) {
        return new Text(o.toString());
    }

    @Override
    public String getPrimitiveJavaObject(final Object o) {
        return o.toString();
    }

    @Override
    protected String getValidatedPrimitiveJavaObject(final String fieldName) {
        return fieldName;
    }

    @Override
    protected void validateSize(final String fieldName) {
        // no-op
    }
}