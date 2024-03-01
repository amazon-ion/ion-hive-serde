// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors.factories;

import com.amazon.ionhiveserde.configuration.SerDeProperties;
import com.amazon.ionhiveserde.objectinspectors.IonBooleanToBooleanObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonFloatToDoubleObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonFloatToFloatObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonLobToBinaryObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonNumberToDecimalObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonSequenceToListObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonStructToStructInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTextToCharObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTextToStringObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTextToVarcharObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonUnionObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonValueToStringObjectInspector;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

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
    private static final IonNumberToDecimalObjectInspector NUMBER_TO_DECIMAL_OBJECT_INSPECTOR =
        new IonNumberToDecimalObjectInspector();
    private static final IonTextToStringObjectInspector TEXT_TO_STRING_OBJECT_INSPECTOR =
        new IonTextToStringObjectInspector();
    private static final IonLobToBinaryObjectInspector LOB_TO_BINARY_OBJECT_INSPECTOR =
        new IonLobToBinaryObjectInspector();
    private static final IonValueToStringObjectInspector ION_VALUE_TO_STRING_OBJECT_INSPECTOR =
            new IonValueToStringObjectInspector();

    // Injected object inspectors
    private final TimestampObjectInspector timestampToTimestampObjectInspector;
    private final DateObjectInspector timestampToDateObjectInspector;

    public IonObjectInspectorFactory(
            final DateObjectInspector timestampToDateObjectInspector,
            final TimestampObjectInspector timestampToTimestampObjectInspector
    ) {
        this.timestampToDateObjectInspector = timestampToDateObjectInspector;
        this.timestampToTimestampObjectInspector = timestampToTimestampObjectInspector;
    }

    /**
     * Creates an object inspector for the table correctly configured.
     *
     * @param structTypeInfo type info for the table.
     * @param serDeProperties SerDe properties to configure the object inspector.
     * @return configured object inspector.
     */
    public ObjectInspector objectInspectorForTable(final StructTypeInfo structTypeInfo,
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
    private ObjectInspector objectInspectorForField(final TypeInfo typeInfo,
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
                        objectInspector = NUMBER_TO_DECIMAL_OBJECT_INSPECTOR;
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
                        objectInspector = ION_VALUE_TO_STRING_OBJECT_INSPECTOR;
                        break;

                    case BINARY:
                        objectInspector = LOB_TO_BINARY_OBJECT_INSPECTOR;
                        break;

                    case DATE:
                        objectInspector = timestampToDateObjectInspector;
                        break;

                    case TIMESTAMP:
                        objectInspector = timestampToTimestampObjectInspector;
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
