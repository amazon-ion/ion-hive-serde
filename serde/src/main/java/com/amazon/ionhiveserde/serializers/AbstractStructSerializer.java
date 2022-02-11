/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ionhiveserde.configuration.SerDeProperties;
import com.amazon.ionhiveserde.configuration.SerializeNullStrategy;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * Base class for struct serializers.
 */
abstract class AbstractStructSerializer implements IonSerializer {

    private final SerDeProperties properties;

    AbstractStructSerializer(final SerDeProperties properties) {
        this.properties = properties;
    }

    @Override
    public void serialize(final IonWriter writer,
                          final Object data,
                          final ObjectInspector objectInspector) throws IOException {
        switch (objectInspector.getCategory()) {
            case STRUCT:
                serializeStruct(writer, data, (StructObjectInspector) objectInspector);
                break;

            case MAP:
                serializeMap(writer, data, (MapObjectInspector) objectInspector);
                break;

            default:
                throw new IllegalStateException("Invalid object inspector category " + objectInspector.getCategory());
        }
    }

    private void serializeMap(final IonWriter writer,
                              final Object data,
                              final MapObjectInspector objectInspector) throws IOException {
        if (objectInspector.getMapKeyObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new IllegalStateException("Map keys must be primitive. Invalid object inspector category: "
                    + objectInspector.getCategory());

        }
        final PrimitiveObjectInspector keyObjectInspector = (PrimitiveObjectInspector) objectInspector
                .getMapKeyObjectInspector();

        final ObjectInspector valueObjectInspector = objectInspector.getMapValueObjectInspector();

        writer.stepIn(IonType.STRUCT);

        for (Map.Entry entry : objectInspector.getMap(data).entrySet()) {
            final String key = keyObjectInspector.getPrimitiveJavaObject(entry.getKey()).toString();
            final IonSerializer ionSerializer = IonSerializerFactory.serializerForObjectInspector(
                valueObjectInspector,
                properties);

            serializeFieldName(writer, entry.getValue(), key);
            serializeFieldValue(ionSerializer, writer, entry.getValue(), valueObjectInspector);
        }

        writer.stepOut();
    }

    private void serializeStruct(final IonWriter writer,
                                 final Object data,
                                 final StructObjectInspector objectInspector) throws IOException {
        writer.stepIn(IonType.STRUCT);

        for (StructField field : objectInspector.getAllStructFieldRefs()) {
            final IonSerializer ionSerializer = IonSerializerFactory.serializerForObjectInspector(
                field.getFieldObjectInspector(),
                properties);

            final Object fieldData = objectInspector.getStructFieldData(data, field);

            serializeFieldName(writer, fieldData, field.getFieldName());
            serializeFieldValue(ionSerializer, writer, fieldData, field.getFieldObjectInspector());
        }

        writer.stepOut();
    }

    @Override
    public IonType getIonType() {
        return IonType.STRUCT;
    }

    /**
     * Serialize a field value using the provided serializer.
     */
    void serializeFieldValue(final IonSerializer ionSerializer,
                                       final IonWriter writer,
                                       final Object data,
                                       final ObjectInspector objectInspector) throws IOException {
        if (data == null) {
            switch (properties.getSerializeNull()) {
                case TYPED:
                    writer.writeNull(ionSerializer.getIonType());
                    break;

                case UNTYPED:
                    writer.writeNull();
                    break;

                case OMIT:
                    // do nothing
                    break;
            }
        } else {
            ionSerializer.serialize(writer, data, objectInspector);
        }
    }

    /**
     * Serialize a field name.
     */
    void serializeFieldName(final IonWriter writer,
                                      final Object data,
                                      final String name) {
        if (data != null || properties.getSerializeNull() != SerializeNullStrategy.OMIT) {
            writer.setFieldName(name);
        }
    }
}
