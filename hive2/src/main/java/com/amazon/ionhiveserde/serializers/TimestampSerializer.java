// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import java.io.IOException;
import java.sql.Date;
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

                writer.writeTimestamp(Timestamp.forDateZ(date));
                break;

            case TIMESTAMP:
                final java.sql.Timestamp hiveTimestamp =
                    ((TimestampObjectInspector) objectInspector).getPrimitiveJavaObject(data);

                final Timestamp value = Timestamp.forSqlTimestampZ(hiveTimestamp);
                final Timestamp timestampWithOffset = value.withLocalOffset(offsetInMinutes);
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
