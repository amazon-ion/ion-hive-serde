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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonInt} for the int Hive type
 */
public class IonIntToIntObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements IntObjectInspector {
    IonIntToIntObjectInspector() {
        super(TypeInfoFactory.intTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return new IntWritable(ionValue.intValue());
    }

    private void validateSize(final IonInt ionValue) {
        if (ionValue.getIntegerSize() != IntegerSize.INT) {
            throw new IllegalArgumentException("insufficient precision for " + ionValue.toString() + " as " + this.typeInfo.getTypeName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int get(Object o) {
        return (int) getPrimitiveJavaObject(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return ionValue.intValue();
    }
}
