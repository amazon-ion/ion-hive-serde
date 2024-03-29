// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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
abstract class IonSerializerFactory {

    private final Map<IonType, IonSerializer> serializerByTypeCache;

    {
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

    protected abstract IonSerializer newTimestampSerializer(SerDeProperties properties);

    /**
     * Gets an Ion serializer based on an ion type and serde properties.
     */
    IonSerializer serializerForIon(final IonType ionType, final SerDeProperties properties) {
        IonSerializer ionSerializer = null;

        switch (ionType) {
            case TIMESTAMP:
                ionSerializer = newTimestampSerializer(properties);
                break;

            case LIST:
                ionSerializer = new SequenceSerializer(this, properties, IonType.LIST);
                break;

            case SEXP:
                ionSerializer = new SequenceSerializer(this, properties, IonType.SEXP);
                break;

            case STRUCT:
                ionSerializer = new ColumnStructSerializer(this, properties);
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
    IonSerializer serializerForObjectInspector(final ObjectInspector objectInspector,
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
