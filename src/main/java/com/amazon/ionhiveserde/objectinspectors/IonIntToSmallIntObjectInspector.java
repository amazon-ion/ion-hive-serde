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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ShortWritable;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonInt} for the smallint Hive type
 */
public class IonIntToSmallIntObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements ShortObjectInspector {

    private static int MIN_VALUE = Short.MIN_VALUE;
    private static int MAX_VALUE = Short.MAX_VALUE;

    public IonIntToSmallIntObjectInspector() {
        super(TypeInfoFactory.shortTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return new ShortWritable((short) ionValue.intValue());
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
    public short get(Object o) {
        return (short) getPrimitiveJavaObject(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return (short) ionValue.intValue();
    }
}
