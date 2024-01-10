// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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
    public TableSerializer(
        final IonSerializerFactory ionSerializerFactory,
        final List<String> columnNames, final SerDeProperties properties
    ) {
        super(ionSerializerFactory, properties);
        this.columnNames = columnNames;

        serializerByColumn = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            final String columnName = columnNames.get(i);
            final IonType ionType = properties.serializationIonTypeFor(i);
            serializerByColumn.put(columnName, ionSerializerFactory.serializerForIon(ionType, properties));
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


