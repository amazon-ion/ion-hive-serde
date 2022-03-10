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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class IonFieldNameToDecimalObjectInspector
        extends AbstractFieldNameObjectInspector<HiveDecimal>
        implements HiveDecimalObjectInspector {

    public IonFieldNameToDecimalObjectInspector() {
        super(TypeInfoFactory.decimalTypeInfo);
    }

    @Override
    public HiveDecimal getPrimitiveJavaObject(final Object o) {
        return getPrimitiveJavaObjectFromFieldName(o.toString());
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(final Object o) {
        return new HiveDecimalWritable(getPrimitiveJavaObjectFromFieldName(o.toString()));
    }

    @Override
    protected HiveDecimal getValidatedPrimitiveJavaObject(final String fieldName) {
        try {
            return HiveDecimal.create(IonPrimitiveReader.decimalValue(fieldName));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid format for " + fieldName + " as " + this.typeInfo.getTypeName());
        }
    }
}