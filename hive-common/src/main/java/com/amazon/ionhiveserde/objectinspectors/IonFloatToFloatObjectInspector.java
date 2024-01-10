// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonFloat;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;

/**
 * Adapts an {@link IonFloat} for the float Hive type.
 */
public class IonFloatToFloatObjectInspector extends
        AbstractOverflowablePrimitiveObjectInspector<IonFloat, Float> implements
    FloatObjectInspector {

    public IonFloatToFloatObjectInspector(final boolean failOnOverflow) {
        super(TypeInfoFactory.floatTypeInfo, failOnOverflow);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new FloatWritable(getPrimitiveJavaObjectFromIonValue((IonFloat) o));
    }

    @Override
    protected Float getValidatedPrimitiveJavaObject(final IonFloat ionValue) {
        return ionValue.floatValue();
    }

    @Override
    protected void validateSize(final IonFloat ionFloat) {
        final float f = ionFloat.floatValue();
        final double d = ionFloat.doubleValue();

        if (Double.compare(f, d) != 0) {
            throw new IllegalArgumentException(
                "insufficient precision for " + ionFloat.toString() + " as " + this.typeInfo.getTypeName());
        }
    }

    @Override
    public float get(final Object o) {
        return (float) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObjectFromIonValue((IonFloat) o);
    }
}
