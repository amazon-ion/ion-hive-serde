// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonWriter;
import java.io.IOException;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * Base class for Ion Text serializers.
 */
abstract class AbstractTextSerializer implements IonSerializer {

    @Override
    public final void serialize(final IonWriter writer,
                                final Object data,
                                final ObjectInspector objectInspector) throws IOException {

        String text = null;
        final PrimitiveCategory category = ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
        switch (category) {
            case CHAR:
                final HiveChar hiveChar =
                    ((HiveCharObjectInspector) objectInspector).getPrimitiveJavaObject(data);

                text = hiveChar.getStrippedValue();
                break;

            case STRING:
                text = ((StringObjectInspector) objectInspector).getPrimitiveJavaObject(data);
                break;

            case VARCHAR:
                final HiveVarchar hiveVarchar =
                    ((HiveVarcharObjectInspector) objectInspector).getPrimitiveJavaObject(data);

                text = hiveVarchar.getValue();
                break;

            default:
                throw new IllegalArgumentException("Invalid object category for text serializer: " + category);
        }

        writeText(writer, text);
    }

    /**
     * Write correct value type to the writer.
     *
     * @param writer Ion writer to write to.
     * @param text text to be written.
     */
    protected abstract void writeText(final IonWriter writer, final String text) throws IOException;
}
