// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonFloat;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Adapts an {@link IonFloat} for the double Hive type.
 */
public class IonFloatToDoubleObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    DoubleObjectInspector {

    public IonFloatToDoubleObjectInspector() {
        super(TypeInfoFactory.doubleTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new DoubleWritable(getPrimitiveJavaObject((IonFloat) o));
    }

    @Override
    public double get(final Object o) {
        return (double) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObject((IonFloat) o);
    }

    private double getPrimitiveJavaObject(final IonFloat ionValue) {
        return ionValue.doubleValue();
    }
}
