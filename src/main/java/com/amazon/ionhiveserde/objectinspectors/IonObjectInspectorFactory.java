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

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ionhiveserde.util.LRUCache;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

/**
 * Factory to create Ion object inspectors. Caches object inspectors based on {@link TypeInfo}
 */
public class IonObjectInspectorFactory {

    // FIXME figure out a default
    private static LRUCache<TypeInfo, ObjectInspector> CACHE = new LRUCache<>(50);

    public static ObjectInspector objectInspectorFor(final TypeInfo typeInfo) {
        if (CACHE.containsKey(typeInfo)) {
            return CACHE.get(typeInfo);
        }

        ObjectInspector objectInspector = null;
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
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
                        CharTypeInfo charTypeInfo = (CharTypeInfo) primitiveTypeInfo;
                        objectInspector = new IonTextToCharObjectInspector(charTypeInfo.getLength());
                        break;

                    case VARCHAR:
                        VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) primitiveTypeInfo;
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
                        throw new UnsupportedOperationException("TODO not implemented");

                    case UNKNOWN:
                        throw new UnsupportedOperationException("Unknown primitive category");
                }
                break;
            default:
                throw new UnsupportedOperationException("TODO not implemented");
        }

        CACHE.put(typeInfo, objectInspector);
        return objectInspector;
    }
}
