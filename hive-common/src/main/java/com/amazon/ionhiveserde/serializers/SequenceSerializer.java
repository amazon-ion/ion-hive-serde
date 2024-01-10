// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ionhiveserde.configuration.SerDeProperties;
import java.io.IOException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Serializer for sequences.
 */
class SequenceSerializer implements IonSerializer {
    private final IonSerializerFactory ionSerializerFactory;
    private final SerDeProperties properties;
    private final IonType sequenceType;

    SequenceSerializer(
            final IonSerializerFactory ionSerializerFactory,
            final SerDeProperties properties,
            final IonType sequenceType
    ) {
        this.ionSerializerFactory = ionSerializerFactory;
        this.properties = properties;
        this.sequenceType = sequenceType;
    }

    @Override
    public void serialize(final IonWriter writer,
                          final Object data,
                          final ObjectInspector objectInspector) throws IOException {
        final ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;

        writer.stepIn(sequenceType);

        for (int i = 0; i < listObjectInspector.getListLength(data); i++) {
            final ObjectInspector elementObjectInspector = listObjectInspector.getListElementObjectInspector();
            final IonSerializer elementSerializer = ionSerializerFactory.serializerForObjectInspector(
                elementObjectInspector,
                properties);

            elementSerializer.serialize(writer, listObjectInspector.getListElement(data, i), elementObjectInspector);
        }
        writer.stepOut();

    }

    @Override
    public IonType getIonType() {
        return sequenceType;
    }
}
