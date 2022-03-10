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

import com.amazon.ion.Timestamp;
import com.amazon.ionhiveserde.objectinspectors.utils.IonPrimitiveReader;
import java.sql.Date;
import java.time.LocalDate;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class IonFieldNameToDateObjectInspector
        extends AbstractFieldNameObjectInspector<Date>
        implements DateObjectInspector {

    public IonFieldNameToDateObjectInspector() {
        super(TypeInfoFactory.dateTypeInfo);
    }

    @Override
    public Date getPrimitiveJavaObject(final Object o) {
        return getPrimitiveJavaObjectFromFieldName(o.toString());
    }

    @Override
    public DateWritable getPrimitiveWritableObject(final Object o) {
        return new DateWritable(getPrimitiveJavaObjectFromFieldName(o.toString()));
    }

    @Override
    protected Date getValidatedPrimitiveJavaObject(final String fieldName) {
        try {
            Timestamp ionTimestamp = IonPrimitiveReader.timestampValue(fieldName);
            return Date.valueOf(LocalDate.of(ionTimestamp.getYear(), ionTimestamp.getMonth(), ionTimestamp.getDay()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid format for " + fieldName + " as " + this.typeInfo.getTypeName());
        }
    }
}