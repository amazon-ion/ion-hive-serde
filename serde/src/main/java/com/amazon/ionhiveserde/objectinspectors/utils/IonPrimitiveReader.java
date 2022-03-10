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
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonSystemBuilder;
import com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Set;

public class IonPrimitiveReader {
    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    // Restricted to boolean ion types
    public static boolean booleanValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.BOOLEAN).booleanValue();
    }

    // Restricted to numeric ion types
    public static byte byteValue(final String ionPrimitive) {
        return (byte) buildSingleReader(ionPrimitive, CastType.NUMERIC).longValue();
    }

    public static BigDecimal decimalValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.NUMERIC).bigDecimalValue();
    }

    public static double doubleValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.NUMERIC).doubleValue();
    }

    public static float floatValue(final String ionPrimitive) {
        return (float) buildSingleReader(ionPrimitive, CastType.NUMERIC).doubleValue();
    }

    public static int intValue(final String ionPrimitive) {
        return (int) buildSingleReader(ionPrimitive, CastType.NUMERIC).longValue();
    }

    public static long longValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.NUMERIC).longValue();
    }

    public static short shortValue(final String ionPrimitive) {
        return (short) buildSingleReader(ionPrimitive, CastType.NUMERIC).longValue();
    }

    // Restricted to temporal ion types
    public static Date dateValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.TEMPORAL).dateValue();
    }

    public static Timestamp timestampValue(final String ionPrimitive) {
        Timestamp ts = buildSingleReader(ionPrimitive, CastType.TEMPORAL).timestampValue();
        return ts;
    }

    // No restrictions on ion type
    public static String stringValue(final String ionPrimitive) {
        return buildSingleReader(ionPrimitive, CastType.ALL).stringValue();
    }



    private static IonReader buildSingleReader(final String ionPrimitive, final CastType targetType) {
        IonReader reader = SYSTEM.newReader(ionPrimitive);
        IonType type = reader.next();
        if (!targetType.canCast(type)) {
            throw new IllegalArgumentException(
                    String.format("Unable to cast value %s of type %s as %s type.",
                            ionPrimitive,
                            type.name(),
                            targetType.toString().toLowerCase()));
        }
        return reader;
    }

    private enum CastType {
        ALL,
        BOOLEAN,
        NUMERIC,
        TEMPORAL;

        private static Set<IonType> NUMERIC_TYPE = ImmutableSet.of(IonType.INT, IonType.DECIMAL, IonType.FLOAT);

        public boolean canCast(final IonType type) {
            switch (this) {
                case ALL:
                    return true;
                case BOOLEAN:
                    return type == IonType.BOOL;
                case TEMPORAL:
                    return type == IonType.TIMESTAMP;
                case NUMERIC:
                    return NUMERIC_TYPE.contains(type);
                default:
                    return false;
            }
        }
    }
}
