// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import java.io.IOException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;

/**
 * Float serializer.
 */
class FloatSerializer implements IonSerializer {

    @Override
    public void serialize(final IonWriter writer,
                          final Object data,
                          final ObjectInspector objectInspector) throws IOException {

        final PrimitiveCategory category = ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
        switch (category) {
            case FLOAT:
                writer.writeFloat(((FloatObjectInspector) objectInspector).get(data));
                break;

            case DOUBLE:
                writer.writeFloat(((DoubleObjectInspector) objectInspector).get(data));
                break;

            default:
                throw new IllegalArgumentException("Invalid object category for float serializer: " + category);
        }
    }

    @Override
    public IonType getIonType() {
        return IonType.FLOAT;
    }
}
