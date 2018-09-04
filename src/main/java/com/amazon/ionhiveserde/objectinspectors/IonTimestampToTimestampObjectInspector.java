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

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonValue;

import java.sql.Timestamp;

/**
 * Adapts an {@link IonTimestamp} for the timestamp Hive type
 */
public class IonTimestampToTimestampObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements TimestampObjectInspector {

    public IonTimestampToTimestampObjectInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimestampWritable getPrimitiveWritableObject(Object o) {
        if (isIonNull((IonValue) o)) return null;

        return new TimestampWritable(getPrimitiveJavaObject((IonTimestamp) o));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Timestamp getPrimitiveJavaObject(Object o) {
        if (isIonNull((IonValue) o)) return null;

        return getPrimitiveJavaObject((IonTimestamp) o);
    }

    private Timestamp getPrimitiveJavaObject(final IonTimestamp ionValue) {
        // Hive timestamps don't have offset so we always map the ion timestamp to UTC
        // IonTimestamp.getMillis() is milliseconds from 1970-01-01T00:00:00.000Z
        return new Timestamp(ionValue.getMillis());
    }
}
