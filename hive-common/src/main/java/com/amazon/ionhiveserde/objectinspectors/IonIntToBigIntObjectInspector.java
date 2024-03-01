// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * Adapts an {@link IonInt} for the bigint Hive type.
 */
public class IonIntToBigIntObjectInspector extends AbstractOverflowablePrimitiveObjectInspector<IonInt, Long> implements
    LongObjectInspector {

    public static final long MIN_VALUE = Long.MIN_VALUE;
    public static final long MAX_VALUE = Long.MAX_VALUE;

    public IonIntToBigIntObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.longTypeInfo, failOnOverflow);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new LongWritable(getPrimitiveJavaObjectFromIonValue((IonInt) o));
    }

    @Override
    protected Long getValidatedPrimitiveJavaObject(final IonInt ionValue) {
        return ionValue.longValue();
    }

    @Override
    protected void validateSize(final IonInt ionValue) {
        if (ionValue.getIntegerSize() == IntegerSize.BIG_INTEGER) {
            throw new IllegalArgumentException(
                "insufficient precision for " + ionValue.toString() + " as " + this.typeInfo.getTypeName());
        }
    }

    @Override
    public long get(final Object o) {
        return (long) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObjectFromIonValue((IonInt) o);
    }
}
