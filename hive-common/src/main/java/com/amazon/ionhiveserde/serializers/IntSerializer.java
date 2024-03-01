// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import java.io.IOException;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;

/**
 * Int serializer.
 */
class IntSerializer implements IonSerializer {

    @Override
    public void serialize(final IonWriter writer,
                          final Object data,
                          final ObjectInspector objectInspector) throws IOException {

        final PrimitiveCategory category = ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
        switch (category) {
            case BYTE:
                writer.writeInt(((ByteObjectInspector) objectInspector).get(data));
                break;

            case SHORT:
                writer.writeInt(((ShortObjectInspector) objectInspector).get(data));
                break;

            case INT:
                writer.writeInt(((IntObjectInspector) objectInspector).get(data));
                break;

            case LONG:
                writer.writeInt(((LongObjectInspector) objectInspector).get(data));
                break;

            case DECIMAL:
                final HiveDecimal hiveDecimal =
                    ((HiveDecimalObjectInspector) objectInspector).getPrimitiveJavaObject(data);

                writer.writeInt(hiveDecimal.bigDecimalValue().toBigInteger());
                break;

            default:
                throw new IllegalArgumentException("Invalid object category for int serializer: " + category);
        }
    }

    @Override
    public IonType getIonType() {
        return IonType.INT;
    }
}
