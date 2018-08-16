/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.amazon.ionhiveserde;

import com.amazon.ionhiveserde.objectinspectors.IonStructObjectInspector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonSystemBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/*
TODO
Move null handling to abstract class
add OI interfaces
*/

/**
 * <p>
 * Hive SerDe for the <a href="http://amzn.github.io/ion-docs/docs.html">Amazon Ion</a> data format
 * </p>
 * <p>
 * For more information on Hive SerDes see <a href="https://cwiki.apache.org/confluence/display/Hive/SerDe">wiki</a>
 * </p>
 */
public class IonHiveSerDe extends AbstractSerDe {
    public static final Log LOGGER = LogFactory.getLog(IonHiveSerDe.class);

    private IonSystem ion;

    private IonStructObjectInspector objectInspector;
    private Class<? extends Writable> serializedClass;

    private SerDeStats stats;

    @Override
    public void initialize(final @Nullable Configuration conf, final Properties tbl) throws SerDeException {

        stats = new SerDeStats();
        StructTypeInfo tableInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(readColumnNames(tbl), readColumnTypes(tbl));

        ion = buildIonSystem(conf);

        objectInspector = new IonStructObjectInspector(tableInfo); // TODO get from the OI factory

        serializedClass = Text.class; // TODO pick from config
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return serializedClass;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objectInspector) throws SerDeException {
        final StringBuilder out = new StringBuilder();
        final StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;

        try (IonWriter writer = ion.newTextWriter(out)) {
            writer.stepIn(IonType.STRUCT);

            for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                writer.setFieldName(field.getFieldName());

                Object fieldData = structObjectInspector.getStructFieldData(obj, field);
                serializeFieldData(writer, fieldData, field.getFieldObjectInspector());
            }

            writer.stepOut();
        } catch (IOException e) {
            throw new SerDeException(e);
        }

        return new Text(out.toString());
    }

    private void serializeFieldData(IonWriter writer, Object fieldData, ObjectInspector objectInspector) throws IOException, SerDeException {
        if (fieldData == null) return;

        switch (objectInspector.getCategory()) {
            case PRIMITIVE:
                serializePrimitiveFieldData(writer, fieldData, (PrimitiveObjectInspector) objectInspector);
                break;
            default:
                throw new UnsupportedOperationException("TODO not implemented");
        }
    }

    private void serializePrimitiveFieldData(IonWriter writer, Object fieldData, PrimitiveObjectInspector objectInspector) throws IOException, SerDeException {
        switch (objectInspector.getPrimitiveCategory()) {
            case VOID:
                throw new UnsupportedOperationException("TODO not implemented");
            case UNKNOWN:
                throw new SerDeException("Unknown primitive");
            default:
                IonValue ionValue = (IonValue) fieldData;
                IonReader ionReader = ion.newReader(ionValue);
                ionReader.next();
                writer.writeValue(ionReader);
                break;
        }
    }

    @Override
    public SerDeStats getSerDeStats() {
        return stats;
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        // TODO make a wrapper over IonValue to be able to make it lazy with a reader.
        IonValue value;

        if (blob instanceof Text) {
            final Text text = (Text) blob;

            stats.setRawDataSize(text.getBytes().length);

            String rawText = text.toString().trim();
            value = ion.getLoader().load(rawText).get(0);
        } else if (blob instanceof ByteWritable) {
            throw new UnsupportedOperationException("TODO not implemented");
        } else {
            throw new UnsupportedOperationException("TODO not implemented");
        }

        return value;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    private List<String> readColumnNames(final Properties tbl) {
        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);

        if (columnNameProperty.isEmpty()) {
            return new ArrayList<>();
        }

        return Arrays.asList(columnNameProperty.split(","));
    }

    private List<TypeInfo> readColumnTypes(final Properties tbl) {
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        if (columnTypeProperty.isEmpty()) {
            return new ArrayList<>();
        }

        return TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    private IonSystem buildIonSystem(final @Nullable Configuration conf) {
        // TODO configure it from SERDEPROPERTIES

        return IonSystemBuilder.standard().build();
    }
}

