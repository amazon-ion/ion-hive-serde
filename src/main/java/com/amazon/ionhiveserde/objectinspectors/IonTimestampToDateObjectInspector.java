/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.objectinspectors;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonValue;

import java.sql.Date;

/**
 * Adapts an {@link IonTimestamp} for the date Hive type
 */
public class IonTimestampToDateObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements DateObjectInspector {

    public IonTimestampToDateObjectInspector() {
        super(TypeInfoFactory.dateTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DateWritable getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        return new DateWritable(getPrimitiveJavaObject((IonTimestamp) o));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        return getPrimitiveJavaObject((IonTimestamp) o);
    }

    private Date getPrimitiveJavaObject(final IonTimestamp ionValue) {
        return new Date(ionValue.getMillis());
    }
}

