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

package software.amazon.ionhiveserde;

import java.io.IOException;
import java.sql.Date;
import java.util.Map;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.Timestamp;

class Serializer {

    static void serializeStruct(final IonWriter writer,
                                final Object data,
                                final StructObjectInspector objectInspector) throws IOException, SerDeException {
        writer.stepIn(IonType.STRUCT);
        for (StructField field : objectInspector.getAllStructFieldRefs()) {
            final Object fieldData = objectInspector.getStructFieldData(data, field);
            serializeField(writer, field.getFieldName(), fieldData, field.getFieldObjectInspector());
        }
        writer.stepOut();
    }

    private static void serializeField(final IonWriter writer,
                                       final String fieldName,
                                       final Object fieldData,
                                       final ObjectInspector objectInspector) throws SerDeException, IOException {
        if (fieldData == null) {
            return;
        }

        writer.setFieldName(fieldName);

        serializeValue(writer, fieldData, objectInspector);
    }

    private static void serializeValue(final IonWriter writer,
                                       final Object data,
                                       final ObjectInspector objectInspector) throws IOException, SerDeException {
        if (data == null) {
            writer.writeNull();
        } else {
            switch (objectInspector.getCategory()) {
                case PRIMITIVE:
                    serializePrimitive(writer, data, (PrimitiveObjectInspector) objectInspector);
                    break;

                case MAP:
                    serializeMap(writer, data, (MapObjectInspector) objectInspector);
                    break;

                case UNION:
                    serializeUnion(writer, data, (UnionObjectInspector) objectInspector);
                    break;

                case STRUCT:
                    serializeStruct(writer, data, (StructObjectInspector) objectInspector);
                    break;

                case LIST:
                    serializeList(writer, data, (ListObjectInspector) objectInspector);
                    break;
            }
        }
    }

    private static void serializeUnion(final IonWriter writer,
                                       final Object data,
                                       final UnionObjectInspector objectInspector) throws IOException, SerDeException {
        final byte tag = objectInspector.getTag(data);
        final ObjectInspector fieldObjectInspector = objectInspector.getObjectInspectors().get(tag);

        serializeValue(writer, data, fieldObjectInspector);
    }

    private static void serializeList(final IonWriter writer,
                                      final Object data,
                                      final ListObjectInspector objectInspector) throws IOException, SerDeException {
        final ObjectInspector listElementObjectInspector = objectInspector.getListElementObjectInspector();

        writer.stepIn(IonType.LIST);
        for (int i = 0; i < objectInspector.getListLength(data); i++) {
            serializeValue(writer, objectInspector.getListElement(data, i), listElementObjectInspector);
        }
        writer.stepOut();
    }

    private static void serializeMap(final IonWriter writer,
                                     final Object data,
                                     final MapObjectInspector mapObjectInspector) throws IOException, SerDeException {
        final StringObjectInspector keyObjectInspector =
            (StringObjectInspector) mapObjectInspector.getMapKeyObjectInspector();
        final ObjectInspector valueObjectInspector = mapObjectInspector.getMapValueObjectInspector();

        writer.stepIn(IonType.STRUCT);
        for (Map.Entry entry : mapObjectInspector.getMap(data).entrySet()) {
            final String key = keyObjectInspector.getPrimitiveJavaObject(entry.getKey());
            serializeField(writer, key, entry.getValue(), valueObjectInspector);
        }
        writer.stepOut();
    }


    private static void serializePrimitive(final IonWriter writer,
                                           final Object fieldData,
                                           final PrimitiveObjectInspector primitiveObjectInspector)
        throws SerDeException,
        IOException {
        switch (primitiveObjectInspector.getPrimitiveCategory()) {
            case BOOLEAN:
                writer.writeBool(((BooleanObjectInspector) primitiveObjectInspector).get(fieldData));
                break;

            case BYTE:
                writer.writeInt(((ByteObjectInspector) primitiveObjectInspector).get(fieldData));
                break;

            case SHORT:
                writer.writeInt(((ShortObjectInspector) primitiveObjectInspector).get(fieldData));
                break;

            case INT:
                writer.writeInt(((IntObjectInspector) primitiveObjectInspector).get(fieldData));
                break;

            case LONG:
                writer.writeInt(((LongObjectInspector) primitiveObjectInspector).get(fieldData));
                break;

            case FLOAT:
                writer.writeFloat(((FloatObjectInspector) primitiveObjectInspector).get(fieldData));
                break;

            case DOUBLE:
                writer.writeFloat(((DoubleObjectInspector) primitiveObjectInspector).get(fieldData));
                break;

            case DECIMAL:
                final HiveDecimal hiveDecimal =
                    ((HiveDecimalObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(fieldData);

                writer.writeDecimal(hiveDecimal.bigDecimalValue());
                break;

            case DATE:
                final Date date = ((DateObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(fieldData);

                writer.writeTimestamp(Timestamp.forDateZ(date));
                break;

            case TIMESTAMP:
                final java.sql.Timestamp hiveTimestamp =
                    ((TimestampObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(fieldData);

                writer.writeTimestamp(Timestamp.forSqlTimestampZ(hiveTimestamp));
                break;

            case CHAR:
                final HiveChar hiveChar =
                    ((HiveCharObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(fieldData);

                writer.writeString(hiveChar.getStrippedValue());
                break;

            case STRING:
                final String string =
                    ((StringObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(fieldData);

                writer.writeString(string);
                break;

            case VARCHAR:
                final HiveVarchar hiveVarchar =
                    ((HiveVarcharObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(fieldData);

                writer.writeString(hiveVarchar.getValue());
                break;

            case BINARY:
                final byte[] bytes =
                    ((BinaryObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(fieldData);

                writer.writeBlob(bytes);
                break;

            case UNKNOWN:
                throw new SerDeException("Unknown primitive category: "
                    + primitiveObjectInspector.getPrimitiveCategory());
        }
    }
}
