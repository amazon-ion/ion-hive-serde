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
import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class IonFieldNameToTimestampObjectInspector
        extends AbstractFieldNameObjectInspector<Timestamp>
        implements TimestampObjectInspector {

    public IonFieldNameToTimestampObjectInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    @Override
    public Timestamp getPrimitiveJavaObject(final Object o) {
        if (o instanceof Timestamp) {
            return (Timestamp) o;
        }
        return getPrimitiveJavaObjectFromFieldName(o.toString());
    }

    @Override
    public TimestampWritable getPrimitiveWritableObject(final Object o) {
        if (o instanceof Timestamp) {
            return new TimestampWritable((Timestamp) o);
        }
        return new TimestampWritable(getPrimitiveJavaObjectFromFieldName(o.toString()));
    }

    @Override
    protected Timestamp getValidatedPrimitiveJavaObject(final String fieldName) {
        try {
            return new Timestamp(IonPrimitiveReader.timestampValue(fieldName).getMillis());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid format for " + fieldName + " as " + this.typeInfo.getTypeName());
        }
    }
}