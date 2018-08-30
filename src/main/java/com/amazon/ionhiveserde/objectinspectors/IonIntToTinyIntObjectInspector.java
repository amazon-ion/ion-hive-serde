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

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonInt} for the tinyint Hive type
 */
public class IonIntToTinyIntObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements ByteObjectInspector {

    private static int MIN_VALUE = -128;
    private static int MAX_VALUE = 127;

    public IonIntToTinyIntObjectInspector() {
        super(TypeInfoFactory.byteTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return new ByteWritable((byte) ionValue.intValue());
    }

    private void validateSize(final IonInt ionValue) {
        boolean correctIntSize = ionValue.getIntegerSize() == IntegerSize.INT;

        if (!correctIntSize || !validRange(ionValue)) {
            throw new IllegalArgumentException("insufficient precision for " + ionValue.toString() + " as " + this.typeInfo.getTypeName());
        }
    }

    private boolean validRange(IonInt ionValue) {
        // runs after checking that fits in a Java int
        int intValue = ionValue.intValue();
        return MIN_VALUE <= intValue && intValue <= MAX_VALUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte get(Object o) {
        return (byte) getPrimitiveJavaObject(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return (byte) ionValue.intValue();
    }
}
