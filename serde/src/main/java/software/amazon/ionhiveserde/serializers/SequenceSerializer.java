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

package software.amazon.ionhiveserde.serializers;

import java.io.IOException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ionhiveserde.configuration.SerDeProperties;

/**
 * Serializer for sequences.
 */
class SequenceSerializer implements IonSerializer {
    private final SerDeProperties properties;
    private final IonType sequenceType;

    SequenceSerializer(final SerDeProperties properties, final IonType sequenceType) {
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
            final IonSerializer elementSerializer = IonSerializerFactory.serializerForObjectInspector(
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
