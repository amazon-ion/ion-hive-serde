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
