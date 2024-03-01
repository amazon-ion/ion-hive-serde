// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * Adapts an {@link IonBool} for the boolean Hive type.
 */
public class IonBooleanToBooleanObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    BooleanObjectInspector {

    public IonBooleanToBooleanObjectInspector() {
        super(TypeInfoFactory.booleanTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        return new BooleanWritable(getPrimitiveJavaObject((IonBool) o));
    }

    @Override
    public boolean get(final Object o) {
        return (boolean) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObject((IonBool) o);
    }

    public boolean getPrimitiveJavaObject(final IonBool ionValue) {
        return ionValue.booleanValue();
    }
}


