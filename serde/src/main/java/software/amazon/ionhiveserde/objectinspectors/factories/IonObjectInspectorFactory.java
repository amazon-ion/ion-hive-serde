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

package software.amazon.ionhiveserde.objectinspectors.factories;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import software.amazon.ionhiveserde.SerDeProperties;
import software.amazon.ionhiveserde.objectinspectors.IonBooleanToBooleanObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonDecimalToDecimalObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonFloatToDoubleObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonFloatToFloatObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonLobToBinaryObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonSequenceToListObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonStructToStructInspector;
import software.amazon.ionhiveserde.objectinspectors.IonTextToCharObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonTextToStringObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonTextToVarcharObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonTimestampToDateObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonTimestampToTimestampObjectInspector;
import software.amazon.ionhiveserde.objectinspectors.IonUnionObjectInspector;

/**
 * Factory to create Ion object inspectors. Caches object inspectors based on {@link TypeInfo}.
 */
public class IonObjectInspectorFactory {

    // Non configurable object inspectors
    private static final IonBooleanToBooleanObjectInspector BOOLEAN_TO_BOOLEAN_OBJECT_INSPECTOR =
        new IonBooleanToBooleanObjectInspector();
    private static final IonIntToTinyIntObjectInspector INT_TO_TINYINT_FAIL_OBJECT_INSPECTOR =
        new IonIntToTinyIntObjectInspector(true);
    private static final IonIntToTinyIntObjectInspector INT_TO_TINYINT_TRUNCATE_OBJECT_INSPECTOR =
        new IonIntToTinyIntObjectInspector(false);
    private static final IonIntToSmallIntObjectInspector INT_TO_SMALLINT_FAIL_OBJECT_INSPECTOR =
        new IonIntToSmallIntObjectInspector(true);
    private static final IonIntToSmallIntObjectInspector INT_TO_SMALLINT_TRUNCATE_OBJECT_INSPECTOR =
        new IonIntToSmallIntObjectInspector(false);
    private static final IonIntToIntObjectInspector INT_TO_INT_FAIL_OBJECT_INSPECTOR =
        new IonIntToIntObjectInspector(true);
    private static final IonIntToIntObjectInspector INT_TO_INT_TRUNCATE_OBJECT_INSPECTOR =
        new IonIntToIntObjectInspector(false);
    private static final IonIntToBigIntObjectInspector INT_TO_BIGINT_FAIL_OBJECT_INSPECTOR =
        new IonIntToBigIntObjectInspector(true);
    private static final IonIntToBigIntObjectInspector INT_TO_BIGINT_TRUNCATE_OBJECT_INSPECTOR =
        new IonIntToBigIntObjectInspector(false);
    private static final IonFloatToFloatObjectInspector FLOAT_TO_FLOAT_FAIL_OBJECT_INSPECTOR =
        new IonFloatToFloatObjectInspector(true);
    private static final IonFloatToFloatObjectInspector FLOAT_TO_FLOAT_TRUNCATE_OBJECT_INSPECTOR =
        new IonFloatToFloatObjectInspector(false);
    private static final IonFloatToDoubleObjectInspector FLOAT_TO_DOUBLE_OBJECT_INSPECTOR =
        new IonFloatToDoubleObjectInspector();
    private static final IonDecimalToDecimalObjectInspector DECIMAL_TO_DECIMAL_OBJECT_INSPECTOR =
        new IonDecimalToDecimalObjectInspector();
    private static final IonTextToStringObjectInspector TEXT_TO_STRING_OBJECT_INSPECTOR =
        new IonTextToStringObjectInspector();
    private static final IonLobToBinaryObjectInspector LOB_TO_BINARY_OBJECT_INSPECTOR =
        new IonLobToBinaryObjectInspector();
    private static final IonTimestampToDateObjectInspector TIMESTAMP_TO_DATE_OBJECT_INSPECTOR =
        new IonTimestampToDateObjectInspector();
    private static final IonTimestampToTimestampObjectInspector TIMESTAMP_TO_TIMESTAMP_OBJECT_INSPECTOR =
        new IonTimestampToTimestampObjectInspector();

    /**
     * Creates an object inspector for the table correctly configured.
     *
     * @param structTypeInfo type info for the table.
     * @param serDeProperties SerDe properties to configure the object inspector.
     * @return configured object inspector.
     */
    public static ObjectInspector objectInspectorForTable(final StructTypeInfo structTypeInfo,
                                                          final SerDeProperties serDeProperties) {
        final List<ObjectInspector> fieldObjectInspectors = new ArrayList<>();

        final ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        final ArrayList<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();

        for (int i = 0; i < fieldTypeInfos.size(); i++) {
            final ObjectInspector fieldObjectInspector = objectInspectorForField(
                fieldTypeInfos.get(i),
                fieldNames.get(i),
                serDeProperties);
            fieldObjectInspectors.add(fieldObjectInspector);
        }

        return new IonStructToStructInspector(structTypeInfo, fieldObjectInspectors);
    }


    /**
     * Creates an object inspector for a field correctly configured.
     *
     * @param typeInfo type info for the field.
     * @param fieldName field name
     * @param serDeProperties SerDe properties to configure the object inspector.
     * @return configured object inspector.
     */
    private static ObjectInspector objectInspectorForField(final TypeInfo typeInfo,
                                                           final String fieldName,
                                                           final SerDeProperties serDeProperties) {
        final boolean failOnOverflow = serDeProperties.failOnOverflowFor(fieldName);

        ObjectInspector objectInspector = null;
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;

                switch (primitiveTypeInfo.getPrimitiveCategory()) {
                    case BOOLEAN:
                        objectInspector = BOOLEAN_TO_BOOLEAN_OBJECT_INSPECTOR;
                        break;

                    case BYTE:
                        objectInspector = failOnOverflow
                            ? INT_TO_TINYINT_FAIL_OBJECT_INSPECTOR
                            : INT_TO_TINYINT_TRUNCATE_OBJECT_INSPECTOR;
                        break;

                    case SHORT:
                        objectInspector = failOnOverflow
                            ? INT_TO_SMALLINT_FAIL_OBJECT_INSPECTOR
                            : INT_TO_SMALLINT_TRUNCATE_OBJECT_INSPECTOR;
                        break;

                    case INT:
                        objectInspector = failOnOverflow
                            ? INT_TO_INT_FAIL_OBJECT_INSPECTOR
                            : INT_TO_INT_TRUNCATE_OBJECT_INSPECTOR;
                        break;

                    case LONG:
                        objectInspector = failOnOverflow
                            ? INT_TO_BIGINT_FAIL_OBJECT_INSPECTOR
                            : INT_TO_BIGINT_TRUNCATE_OBJECT_INSPECTOR;
                        break;

                    case DECIMAL:
                        // TODO can be decimal or int, needs configuration. Fixing to decimal for now
                        objectInspector = DECIMAL_TO_DECIMAL_OBJECT_INSPECTOR;
                        break;

                    case FLOAT:
                        objectInspector = failOnOverflow
                            ? FLOAT_TO_FLOAT_FAIL_OBJECT_INSPECTOR
                            : FLOAT_TO_FLOAT_TRUNCATE_OBJECT_INSPECTOR;
                        break;

                    case DOUBLE:
                        objectInspector = FLOAT_TO_DOUBLE_OBJECT_INSPECTOR;
                        break;

                    case CHAR:
                        final CharTypeInfo charTypeInfo = (CharTypeInfo) primitiveTypeInfo;
                        objectInspector = new IonTextToCharObjectInspector(charTypeInfo.getLength(), failOnOverflow);
                        break;

                    case VARCHAR:
                        final VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) primitiveTypeInfo;
                        objectInspector = new IonTextToVarcharObjectInspector(
                            varcharTypeInfo.getLength(),
                            failOnOverflow);
                        break;

                    case STRING:
                        objectInspector = TEXT_TO_STRING_OBJECT_INSPECTOR;
                        break;

                    case BINARY:
                        objectInspector = LOB_TO_BINARY_OBJECT_INSPECTOR;
                        break;

                    case DATE:
                        objectInspector = TIMESTAMP_TO_DATE_OBJECT_INSPECTOR;
                        break;

                    case TIMESTAMP:
                        objectInspector = TIMESTAMP_TO_TIMESTAMP_OBJECT_INSPECTOR;
                        break;

                    case VOID:
                    case UNKNOWN:
                        throw new UnsupportedOperationException("Unknown primitive category");
                }
                break;

            case STRUCT:
                final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                final ArrayList<String> structFieldNames = structTypeInfo.getAllStructFieldNames();
                final ArrayList<TypeInfo> structFieldTypeInfo = structTypeInfo.getAllStructFieldTypeInfos();

                final List<ObjectInspector> fieldObjectInspectors = new ArrayList<>(structFieldNames.size());

                for (int i = 0; i < structFieldNames.size(); i++) {
                    final TypeInfo fieldTypeInfo = structFieldTypeInfo.get(i);

                    fieldObjectInspectors.add(i, objectInspectorForField(fieldTypeInfo, fieldName, serDeProperties));
                }

                objectInspector = new IonStructToStructInspector(structTypeInfo, fieldObjectInspectors);

                break;
            case MAP:
                final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;

                // FIXME validate key must be string
                final ObjectInspector valueObjectInspector = objectInspectorForField(
                    mapTypeInfo.getMapValueTypeInfo(),
                    fieldName,
                    serDeProperties);

                objectInspector = new IonStructToMapObjectInspector(valueObjectInspector);
                break;

            case LIST:
                final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                final ObjectInspector elementObjectInspector = objectInspectorForField(
                    listTypeInfo.getListElementTypeInfo(),
                    fieldName,
                    serDeProperties);
                objectInspector = new IonSequenceToListObjectInspector(elementObjectInspector);
                break;

            case UNION:
                final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
                final List<ObjectInspector> objectInspectors = new ArrayList<>(
                    unionTypeInfo.getAllUnionObjectTypeInfos().size());
                for (TypeInfo type : unionTypeInfo.getAllUnionObjectTypeInfos()) {
                    objectInspectors.add(objectInspectorForField(type, fieldName, serDeProperties));
                }
                objectInspector = new IonUnionObjectInspector(objectInspectors);
                break;
        }

        return objectInspector;
    }
}
