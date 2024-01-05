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
