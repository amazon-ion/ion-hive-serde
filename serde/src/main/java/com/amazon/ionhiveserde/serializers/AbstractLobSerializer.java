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
