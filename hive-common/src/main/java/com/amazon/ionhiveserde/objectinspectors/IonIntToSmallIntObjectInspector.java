// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ShortWritable;

/**
 * Adapts an {@link IonInt} for the smallint Hive type.
 */
public class IonIntToSmallIntObjectInspector extends
    AbstractOverflowablePrimitiveObjectInspector<IonInt, Short> implements
    ShortObjectInspector {

    public static final int MIN_VALUE = Short.MIN_VALUE;
    public static final int MAX_VALUE = Short.MAX_VALUE;

    public IonIntToSmallIntObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.shortTypeInfo, failOnOverflow);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new ShortWritable(getPrimitiveJavaObjectFromIonValue((IonInt) o));
    }

    @Override
    protected Short getValidatedPrimitiveJavaObject(final IonInt ionValue) {
        return (short) ionValue.intValue();
    }

    @Override
    protected void validateSize(final IonInt ionValue) {
        boolean correctIntSize = ionValue.getIntegerSize() == IntegerSize.INT;

        if (!correctIntSize || !validRange(ionValue)) {
            throw new IllegalArgumentException(
                "insufficient precision for " + ionValue.toString() + " as " + this.typeInfo.getTypeName());
        }
    }

    private boolean validRange(final IonInt ionValue) {
        // runs after checking that fits in a Java int
        int intValue = ionValue.intValue();
        return MIN_VALUE <= intValue && intValue <= MAX_VALUE;
    }

    @Override
    public short get(final Object o) {
        return (short) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObjectFromIonValue((IonInt) o);
    }
}
