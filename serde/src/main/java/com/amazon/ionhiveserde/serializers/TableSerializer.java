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
import com.amazon.ionhiveserde.configuration.SerDeProperties;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Serializes an object to Ion struct using Hive ObjectInspectors to extract information.
 */
public class TableSerializer extends AbstractStructSerializer {

    private final Map<String, IonSerializer> serializerByColumn;
    private final List<String> columnNames;

    /**
     * Constructor.
     *
     * @param columnNames table column names
     * @param properties SerDe properties
     */
    public TableSerializer(final List<String> columnNames, final SerDeProperties properties) {
        super(properties);
        this.columnNames = columnNames;

        serializerByColumn = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            final String columnName = columnNames.get(i);
            final IonType ionType = properties.serializationIonTypeFor(i);
            serializerByColumn.put(columnName, IonSerializerFactory.serializerForIon(ionType, properties));
        }
    }

    /**
     * Serializes a table row as an Ion struct.
     *
     * @param writer writer used to serialize the data
     * @param data hive provided object
     * @param objectInspector object inspector used to inspect the data parameter
     */
    @Override
    public void serialize(final IonWriter writer,
                          final Object data,
                          final ObjectInspector objectInspector) throws IOException {

        final StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;

        writer.stepIn(IonType.STRUCT);

        final List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
            final StructField field = fields.get(i);
            final String columnName = columnNames.get(i);
            final IonSerializer ionSerializer = serializerByColumn.get(columnName);
            final Object fieldData = structObjectInspector.getStructFieldData(data, field);

            serializeFieldName(writer, fieldData, columnName);
            serializeFieldValue(ionSerializer, writer, fieldData, field.getFieldObjectInspector());
        }

        writer.stepOut();
    }
}


