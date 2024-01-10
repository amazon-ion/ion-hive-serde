// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Adapts an {@link IonInt} for the tinyint Hive type.
 */
public class IonIntToTinyIntObjectInspector extends
        AbstractOverflowablePrimitiveObjectInspector<IonInt, Byte> implements
    ByteObjectInspector {

    public static final int MIN_VALUE = -128;
    public static final int MAX_VALUE = 127;

    public IonIntToTinyIntObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.byteTypeInfo, failOnOverflow);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new ByteWritable(getPrimitiveJavaObjectFromIonValue((IonInt) o));
    }

    @Override
    protected Byte getValidatedPrimitiveJavaObject(final IonInt ionValue) {
        return (byte) ionValue.intValue();
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
    public byte get(final Object o) {
        return (byte) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObjectFromIonValue((IonInt) o);
    }
}
