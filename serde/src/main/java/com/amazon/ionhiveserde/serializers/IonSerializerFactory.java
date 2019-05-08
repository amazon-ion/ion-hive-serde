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
import com.amazon.ionhiveserde.configuration.SerDeProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * Factory for Ion serializers, reuses serializers when possible.
 */
class IonSerializerFactory {

    private static final Map<IonType, IonSerializer> serializerByTypeCache;

    static {
        serializerByTypeCache = new HashMap<>();
        serializerByTypeCache.put(IonType.BOOL, new BoolSerializer());
        serializerByTypeCache.put(IonType.INT, new IntSerializer());
        serializerByTypeCache.put(IonType.FLOAT, new FloatSerializer());
        serializerByTypeCache.put(IonType.DECIMAL, new DecimalSerializer());
        serializerByTypeCache.put(IonType.STRING, new StringSerializer());
        serializerByTypeCache.put(IonType.SYMBOL, new SymbolSerializer());
        serializerByTypeCache.put(IonType.CLOB, new ClobSerializer());
        serializerByTypeCache.put(IonType.BLOB, new BlobSerializer());
    }

    /**
     * Gets an Ion serializer based on an ion type and serde properties.
     */
    static IonSerializer serializerForIon(final IonType ionType, final SerDeProperties properties) {
        IonSerializer ionSerializer = null;

        switch (ionType) {
            case TIMESTAMP:
                ionSerializer = new TimestampSerializer(properties.getTimestampOffsetInMinutes());
                break;

            case LIST:
                ionSerializer = new SequenceSerializer(properties, IonType.LIST);
                break;

            case SEXP:
                ionSerializer = new SequenceSerializer(properties, IonType.SEXP);
                break;

            case STRUCT:
                ionSerializer = new ColumnStructSerializer(properties);
                break;

            default:
                ionSerializer = serializerByTypeCache.get(ionType);
        }

        if (ionSerializer == null) {
            throw new IllegalStateException("No ion serializer for " + ionType);
        }

        return ionSerializer;
    }

    /**
     * Gets an Ion serializer based on an object inspector and serde properties.
     */
    static IonSerializer serializerForObjectInspector(final ObjectInspector objectInspector,
                                                      final SerDeProperties properties) {
        return serializerForIon(ionTypeFrom(objectInspector), properties);
    }

    private static IonType ionTypeFrom(final ObjectInspector objectInspector) {
        switch (objectInspector.getCategory()) {
            case MAP:
            case STRUCT:
                return IonType.STRUCT;

            case LIST:
                return IonType.LIST;

            case PRIMITIVE:
                final PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;

                switch (primitiveObjectInspector.getPrimitiveCategory()) {
                    case BOOLEAN:
                        return IonType.BOOL;

                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                        return IonType.INT;

                    case FLOAT:
                    case DOUBLE:
                        return IonType.FLOAT;

                    case DECIMAL:
                        return IonType.DECIMAL;

                    case DATE:
                    case TIMESTAMP:
                        return IonType.TIMESTAMP;

                    case CHAR:
                    case STRING:
                    case VARCHAR:
                        return IonType.STRING;

                    case BINARY:
                        return IonType.BLOB;
                }
        }

        return IonType.NULL;
    }
}
