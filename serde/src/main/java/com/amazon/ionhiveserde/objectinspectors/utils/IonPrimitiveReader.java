/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.objectinspectors.utils;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;
import com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Set;

public class IonPrimitiveReader {

    private static final IonReaderBuilder READER_BUILDER = IonReaderBuilder.standard().immutable();

    // Restricted to boolean ion types
    /**
     * Convert a string containing a single serialized IonValue to a boolean.
     * @param ionPrimitive - String with serialized Ion value.
     * @return boolean value.
     */
    public static boolean booleanValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.BOOLEAN).booleanValue();
    }

    // Restricted to numeric ion types
    /**
     * Convert a string containing a single serialized IonValue to a byte.
     * @param ionPrimitive - String with serialized Ion value.
     * @return byte value.
     */
    public static byte byteValue(final String ionPrimitive) {
        return (byte) buildSingleReader(ionPrimitive, CastType.NUMERIC).longValue();
    }

    /**
     * Convert a string containing a single serialized IonValue to a BigDecimal.
     * @param ionPrimitive - String with serialized Ion value.
     * @return BigDecimal value.
     */
    public static BigDecimal decimalValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.NUMERIC).bigDecimalValue();
    }

    /**
     * Convert a string containing a single serialized IonValue to a double.
     * @param ionPrimitive - String with serialized Ion value.
     * @return double value.
     */
    public static double doubleValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.NUMERIC).doubleValue();
    }

    /**
     * Convert a string containing a single serialized IonValue to a float.
     * @param ionPrimitive - String with serialized Ion value.
     * @return float value.
     */
    public static float floatValue(final String ionPrimitive) {
        return (float) buildSingleReader(ionPrimitive, CastType.FLOAT).doubleValue();
    }

    /**
     * Convert a string containing a single serialized IonValue to an int.
     * @param ionPrimitive - - String with serialized Ion value.
     * @return int value.
     */
    public static int intValue(final String ionPrimitive) {
        return (int) buildSingleReader(ionPrimitive, CastType.NUMERIC).longValue();
    }

    /**
     * Convert a string containing a single serialized IonValue to a long.
     * @param ionPrimitive - String with serialized Ion value.
     * @return long value.
     */
    public static long longValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.NUMERIC).longValue();
    }

    /**
     * Convert a string containing a single serialized IonValue to a short.
     * @param ionPrimitive - String with serialized Ion value.
     * @return short value.
     */
    public static short shortValue(final String ionPrimitive) {
        return (short) buildSingleReader(ionPrimitive, CastType.NUMERIC).longValue();
    }

    // Restricted to temporal ion types

    /**
     * Convert a string containing a single serialized IonValue to an Ion timestamp.
     * @param ionPrimitive - String with serialized Ion value.
     * @return Ion timestamp value.
     */
    public static Timestamp timestampValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.TEMPORAL).timestampValue();
    }

    // No restrictions on ion type
    /**
     * Reads a single string containing a serialized IonValue as a string.
     * @param ionPrimitive - String with serialized Ion value.
     * @return String value containing text representation of an IonValue.
     */
    public static String stringValue(final String ionPrimitive) {
        IonReader reader = READER_BUILDER.build(ionPrimitive);
        IonType type = reader.next();
        validateReader(ionPrimitive, type, CastType.STRING);
        return toStringValue(reader, type, ionPrimitive);
    }

    private static IonReader buildSingleReader(final String ionPrimitive, final CastType targetType) {
        IonReader reader = READER_BUILDER.build(ionPrimitive);
        IonType type = reader.next();
        validateReader(ionPrimitive, type, targetType);
        return reader;
    }

    private static void validateReader(final String ionPrimitive, final IonType type, final CastType targetType) {

        if (!targetType.canCast(type)) {
            throw new IllegalArgumentException(
                    String.format("Unable to cast value %s of type %s as %s type.",
                            ionPrimitive,
                            type.name(),
                            targetType.toString().toLowerCase()));
        }
    }

    private static String toStringValue(final IonReader reader, final IonType type, final String ionPrimitive) {
        String stringValue;
        switch (type) {
            case BOOL:
                stringValue = String.valueOf(reader.booleanValue());
                break;
            case DECIMAL:
                stringValue = reader.decimalValue().toString();
                break;
            case FLOAT:
                stringValue = String.valueOf(reader.doubleValue());
                break;
            case INT:
                stringValue = String.valueOf(reader.intValue());
                break;
            case TIMESTAMP:
                stringValue = reader.timestampValue().toString();
                break;
            default:
                stringValue = reader.stringValue();
        }
        return stringValue;
    }

    private enum CastType {
        BOOLEAN,
        FLOAT,
        NUMERIC,
        STRING,
        TEMPORAL;

        private static Set<IonType> NUMERIC_TYPE = ImmutableSet.of(IonType.INT, IonType.DECIMAL, IonType.FLOAT);
        private static Set<IonType> FLOAT_TYPE = ImmutableSet.of(IonType.DECIMAL, IonType.FLOAT);

        public boolean canCast(final IonType type) {
            switch (this) {
                case BOOLEAN:
                    return type == IonType.BOOL;
                case FLOAT:
                    return FLOAT_TYPE.contains(type);
                case NUMERIC:
                    return NUMERIC_TYPE.contains(type);
                case STRING:
                    return true;
                case TEMPORAL:
                    return type == IonType.TIMESTAMP;
                default:
                    return false;
            }
        }
    }
}
