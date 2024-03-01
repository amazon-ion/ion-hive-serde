// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import java.io.IOException;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

/**
 * Timestamp serializer.
 */
class TimestampSerializer implements IonSerializer {

    private final Integer offsetInMinutes;

    TimestampSerializer(final Integer offsetInMinutes) {
        this.offsetInMinutes = offsetInMinutes;
    }

    @Override
    public void serialize(final IonWriter writer,
                          final Object data,
                          final ObjectInspector objectInspector) throws IOException {

        final PrimitiveCategory category = ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
        switch (category) {
            case DATE:
                final Date date = ((DateObjectInspector) objectInspector)
                    .getPrimitiveJavaObject(data);

                Timestamp ionTimestamp = Timestamp.forDay(date.getYear(), date.getMonth(),
                                                     date.getDay());
                writer.writeTimestamp(ionTimestamp);
                break;

            case TIMESTAMP:
                final org.apache.hadoop.hive.common.type.Timestamp hiveTimestamp =
                    ((TimestampObjectInspector) objectInspector).getPrimitiveJavaObject(data);

                final Timestamp timestampWithOffset = Timestamp.forMillis(hiveTimestamp.toEpochMilli(),
                                                            offsetInMinutes);
                writer.writeTimestamp(timestampWithOffset);
                break;

            default:
                throw new IllegalArgumentException("Invalid object category for timestamp serializer: " + category);
        }
    }

    @Override
    public IonType getIonType() {
        return IonType.TIMESTAMP;
    }
}
