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

package com.amazon.ionhiveserde.objectinspectors.factories;

import com.amazon.ionhiveserde.objectinspectors.IonBooleanToBooleanObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonDecimalToDecimalObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonFloatToDoubleObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonFloatToFloatObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToSmallIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonLobToBinaryObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonSequenceToListObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonStructToMapObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonStructToStructInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTextToCharObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTextToStringObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTextToVarcharObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTimestampToDateObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTimestampToTimestampObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonUnionObjectInspector;
import com.amazon.ionhiveserde.util.LruCache;
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

/**
 * Factory to create Ion object inspectors. Caches object inspectors based on {@link TypeInfo}.
 */
public class IonObjectInspectorFactory {

    // FIXME figure out a default
    private static LruCache<TypeInfo, ObjectInspector> CACHE = new LruCache<>(50);

    /**
     * Provides a potentially cached ObjectInspector for the respective typeInfo.
     */
    public static ObjectInspector objectInspectorFor(final TypeInfo typeInfo) {
        if (CACHE.containsKey(typeInfo)) {
            return CACHE.get(typeInfo);
        }

        ObjectInspector objectInspector = null;
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
                switch (primitiveTypeInfo.getPrimitiveCategory()) {
                    case BOOLEAN:
                        objectInspector = new IonBooleanToBooleanObjectInspector();
                        break;

                    case BYTE:
                        objectInspector = new IonIntToTinyIntObjectInspector();
                        break;

                    case SHORT:
                        objectInspector = new IonIntToSmallIntObjectInspector();
                        break;

                    case INT:
                        objectInspector = new IonIntToIntObjectInspector();
                        break;

                    case LONG:
                        objectInspector = new IonIntToBigIntObjectInspector();
                        break;

                    case DECIMAL:
                        // TODO can be decimal or int, needs configuration. Fixing to decimal for now
                        objectInspector = new IonDecimalToDecimalObjectInspector();
                        break;

                    case FLOAT:
                        objectInspector = new IonFloatToFloatObjectInspector();
                        break;

                    case DOUBLE:
                        objectInspector = new IonFloatToDoubleObjectInspector();
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
                        objectInspector = new IonTextToStringObjectInspector();
                        break;

                    case BINARY:
                        objectInspector = new IonLobToBinaryObjectInspector();
                        break;

                    case DATE:
                        objectInspector = new IonTimestampToDateObjectInspector();
                        break;

                    case TIMESTAMP:
                        objectInspector = new IonTimestampToTimestampObjectInspector();
                        break;

                    case VOID:
                    case UNKNOWN:
                        throw new UnsupportedOperationException("Unknown primitive category");
                }
                break;

            case STRUCT:
                objectInspector = new IonStructToStructInspector((StructTypeInfo) typeInfo);
                break;

            case MAP:
                final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;

                // FIXME validate key must be string
                final ObjectInspector valueObjectInspector = IonObjectInspectorFactory
                    .objectInspectorFor(mapTypeInfo.getMapValueTypeInfo());

                objectInspector = new IonStructToMapObjectInspector(valueObjectInspector);
                break;

            case LIST:
                final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                final ObjectInspector elementObjectInspector = IonObjectInspectorFactory
                    .objectInspectorFor(listTypeInfo.getListElementTypeInfo());
                objectInspector = new IonSequenceToListObjectInspector(elementObjectInspector);
                break;

            case UNION:
                final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
                final List<ObjectInspector> objectInspectors = new ArrayList<>(
                    unionTypeInfo.getAllUnionObjectTypeInfos().size());
                for (TypeInfo type : unionTypeInfo.getAllUnionObjectTypeInfos()) {
                    objectInspectors.add(IonObjectInspectorFactory.objectInspectorFor(type));
                }
                objectInspector = new IonUnionObjectInspector(objectInspectors);
                break;
        }

        CACHE.put(typeInfo, objectInspector);
        return objectInspector;
    }
}
