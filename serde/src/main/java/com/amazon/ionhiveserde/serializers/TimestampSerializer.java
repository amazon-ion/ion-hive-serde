/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

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
