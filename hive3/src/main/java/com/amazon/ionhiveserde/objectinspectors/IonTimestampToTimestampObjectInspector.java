// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Adapts an {@link IonTimestamp} for the timestamp Hive type.
 */
public class IonTimestampToTimestampObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    TimestampObjectInspector {

    public IonTimestampToTimestampObjectInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    @Override
    public TimestampWritableV2 getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new TimestampWritableV2(getPrimitiveJavaObject((IonTimestamp) o));
    }

    @Override
    public Timestamp getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObject((IonTimestamp) o);
    }

    private Timestamp getPrimitiveJavaObject(final IonTimestamp ionValue) {
        // Hive timestamps don't have offset so we always map the ion timestamp to UTC
        // IonTimestamp.getMillis() is milliseconds from 1970-01-01T00:00:00.000Z

        return Timestamp.ofEpochMilli(ionValue.getMillis());
    }
}
