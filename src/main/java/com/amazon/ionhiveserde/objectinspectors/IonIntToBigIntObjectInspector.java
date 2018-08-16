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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonValue;

public class IonIntToBigIntObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements LongObjectInspector {

    IonIntToBigIntObjectInspector() {
        super(TypeInfoFactory.longTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return new LongWritable(ionValue.longValue());
    }

    private void validateSize(final IonInt ionValue) {
        if (ionValue.getIntegerSize() == IntegerSize.BIG_INTEGER) {
            throw new IllegalArgumentException("insufficient precision for " + ionValue.toString() + " as " + this.typeInfo.getTypeName());
        }
    }

    @Override
    public long get(Object o) {
        return (long) getPrimitiveJavaObject(o);
    }

    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return ionValue.longValue();
    }
}
