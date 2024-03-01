// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonWriter;
import java.io.IOException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

/**
 * Base class for Ion Lob serializers.
 */
abstract class AbstractLobSerializer implements IonSerializer {

    @Override
    public final void serialize(final IonWriter writer,
                          final Object data,
                          final ObjectInspector objectInspector) throws IOException {

        final PrimitiveCategory category = ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
        switch (category) {
            case BINARY:
                final byte[] bytes =
                    ((BinaryObjectInspector) objectInspector).getPrimitiveJavaObject(data);

                writeValue(writer, bytes);
                break;

            default:
                throw new IllegalArgumentException("Invalid object category for lob serializer: " + category);
        }
    }

    /**
     * Write correct value type to the writer.
     *
     * @param writer Ion writer to write to.
     * @param bytes bytes to be written.
     */
    protected abstract void writeValue(final IonWriter writer, final byte[] bytes) throws IOException;
}
