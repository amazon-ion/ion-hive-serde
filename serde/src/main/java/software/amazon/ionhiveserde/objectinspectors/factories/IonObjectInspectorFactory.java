/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors.factories;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
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
    private static final IonIntToTinyIntObjectInspector INT_TO_TINYINT_OBJECT_INSPECTOR =
        new IonIntToTinyIntObjectInspector();
    private static final IonIntToSmallIntObjectInspector INT_TO_SMALLINT_OBJECT_INSPECTOR =
        new IonIntToSmallIntObjectInspector();
    private static final IonIntToIntObjectInspector INT_TO_INT_OBJECT_INSPECTOR =
        new IonIntToIntObjectInspector();
    private static final IonIntToBigIntObjectInspector INT_TO_BIGINT_OBJECT_INSPECTOR =
        new IonIntToBigIntObjectInspector();
    private static final IonDecimalToDecimalObjectInspector DECIMAL_TO_DECIMAL_OBJECT_INSPECTOR =
        new IonDecimalToDecimalObjectInspector();
    private static final IonFloatToFloatObjectInspector FLOAT_TO_FLOAT_OBJECT_INSPECTOR =
        new IonFloatToFloatObjectInspector();
    private static final IonFloatToDoubleObjectInspector FLOAT_TO_DOUBLE_OBJECT_INSPECTOR =
        new IonFloatToDoubleObjectInspector();
    private static final IonTextToStringObjectInspector TEXT_TO_STRING_OBJECT_INSPECTOR =
        new IonTextToStringObjectInspector();
    private static final IonLobToBinaryObjectInspector LOB_TO_BINARY_OBJECT_INSPECTOR =
        new IonLobToBinaryObjectInspector();
    private static final IonTimestampToDateObjectInspector TIMESTAMP_TO_DATE_OBJECT_INSPECTOR =
        new IonTimestampToDateObjectInspector();
    private static final IonTimestampToTimestampObjectInspector TIMESTAMP_TO_TIMESTAMP_OBJECT_INSPECTOR =
        new IonTimestampToTimestampObjectInspector();


    // each SerDe instance use gets a new cache so the size is proportional to the table columns
    private final Map<TypeInfo, ObjectInspector> cache;

    /**
     * Creates a ObjectInspector factory pre caching the object inspectors for the structTypeInfo.
     *
     * @param structTypeInfo table type info.
     */
    public IonObjectInspectorFactory(final StructTypeInfo structTypeInfo) {
        cache = new HashMap<>();
        for (final TypeInfo typeInfo : structTypeInfo.getAllStructFieldTypeInfos()) {
            // pre populate the cache
            objectInspectorFor(typeInfo);
        }
    }

    /**
     * Provides a potentially cached ObjectInspector for the respective typeInfo.
     */
    public ObjectInspector objectInspectorFor(final TypeInfo typeInfo) {
        if (cache.containsKey(typeInfo)) {
            return cache.get(typeInfo);
        }

        ObjectInspector objectInspector = null;
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
                switch (primitiveTypeInfo.getPrimitiveCategory()) {
                    case BOOLEAN:
                        objectInspector = BOOLEAN_TO_BOOLEAN_OBJECT_INSPECTOR;
                        break;

                    case BYTE:
                        objectInspector = INT_TO_TINYINT_OBJECT_INSPECTOR;
                        break;

                    case SHORT:
                        objectInspector = INT_TO_SMALLINT_OBJECT_INSPECTOR;
                        break;

                    case INT:
                        objectInspector = INT_TO_INT_OBJECT_INSPECTOR;
                        break;

                    case LONG:
                        objectInspector = INT_TO_BIGINT_OBJECT_INSPECTOR;
                        break;

                    case DECIMAL:
                        // TODO can be decimal or int, needs configuration. Fixing to decimal for now
                        objectInspector = DECIMAL_TO_DECIMAL_OBJECT_INSPECTOR;
                        break;

                    case FLOAT:
                        objectInspector = FLOAT_TO_FLOAT_OBJECT_INSPECTOR;
                        break;

                    case DOUBLE:
                        objectInspector = FLOAT_TO_DOUBLE_OBJECT_INSPECTOR;
                        break;

                    case CHAR:
                        final CharTypeInfo charTypeInfo = (CharTypeInfo) primitiveTypeInfo;
                        objectInspector = new IonTextToCharObjectInspector(charTypeInfo.getLength());
                        break;

                    case VARCHAR:
                        final VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) primitiveTypeInfo;
                        objectInspector = new IonTextToVarcharObjectInspector(varcharTypeInfo.getLength());
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

                    fieldObjectInspectors.add(i, objectInspectorFor(fieldTypeInfo));
                }

                objectInspector = new IonStructToStructInspector(structTypeInfo, fieldObjectInspectors);

                break;
            case MAP:
                final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;

                // FIXME validate key must be string
                final ObjectInspector valueObjectInspector = objectInspectorFor(mapTypeInfo.getMapValueTypeInfo());

                objectInspector = new IonStructToMapObjectInspector(valueObjectInspector);
                break;

            case LIST:
                final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                final ObjectInspector elementObjectInspector = objectInspectorFor(
                    listTypeInfo.getListElementTypeInfo());
                objectInspector = new IonSequenceToListObjectInspector(elementObjectInspector);
                break;

            case UNION:
                final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
                final List<ObjectInspector> objectInspectors = new ArrayList<>(
                    unionTypeInfo.getAllUnionObjectTypeInfos().size());
                for (TypeInfo type : unionTypeInfo.getAllUnionObjectTypeInfos()) {
                    objectInspectors.add(objectInspectorFor(type));
                }
                objectInspector = new IonUnionObjectInspector(objectInspectors);
                break;
        }

        cache.put(typeInfo, objectInspector);
        return objectInspector;
    }
}
