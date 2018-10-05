/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.DoubleWritable;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonFloat} for the double Hive type.
 */
public class IonFloatToDoubleObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    DoubleObjectInspector {

    public IonFloatToDoubleObjectInspector() {
        super(TypeInfoFactory.doubleTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new DoubleWritable(getPrimitiveJavaObject((IonFloat) o));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double get(final Object o) {
        return (double) getPrimitiveJavaObject(o);
    }

    /**
     * {@inheritDoc}
     */
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
