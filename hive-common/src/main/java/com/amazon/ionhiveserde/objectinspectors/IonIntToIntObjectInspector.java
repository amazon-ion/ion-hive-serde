// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * Adapts an {@link IonInt} for the int Hive type.
 */
public class IonIntToIntObjectInspector extends AbstractOverflowablePrimitiveObjectInspector<IonInt, Integer> implements
    IntObjectInspector {

    public static final int MIN_VALUE = Integer.MIN_VALUE;
    public static final int MAX_VALUE = Integer.MAX_VALUE;

    public IonIntToIntObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.intTypeInfo, failOnOverflow);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new IntWritable(getPrimitiveJavaObjectFromIonValue((IonInt) o));
    }

    @Override
    protected Integer getValidatedPrimitiveJavaObject(final IonInt ionValue) {
        return ionValue.intValue();
    }

    @Override
    protected void validateSize(final IonInt ionValue) {
        if (ionValue.getIntegerSize() != IntegerSize.INT) {
            throw new IllegalArgumentException(
                "insufficient precision for " + ionValue.toString() + " as " + this.typeInfo.getTypeName());
        }
    }

    @Override
    public int get(final Object o) {
        return (int) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObjectFromIonValue((IonInt) o);
    }
}
