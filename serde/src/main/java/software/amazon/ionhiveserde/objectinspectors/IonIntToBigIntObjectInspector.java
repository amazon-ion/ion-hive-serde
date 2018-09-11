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

package software.amazon.ionhiveserde.objectinspectors;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonInt} for the bigint Hive type.
 */
public class IonIntToBigIntObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    LongObjectInspector {

    public IonIntToBigIntObjectInspector() {
        super(TypeInfoFactory.longTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        IonInt ionValue = (IonInt) o;
        validateSize(ionValue);
        return new LongWritable(getPrimitiveJavaObject((IonInt) o));
    }

    private void validateSize(final IonInt ionValue) {
        if (ionValue.getIntegerSize() == IntegerSize.BIG_INTEGER) {
            throw new IllegalArgumentException(
                "insufficient precision for " + ionValue.toString() + " as " + this.typeInfo.getTypeName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long get(final Object o) {
        return (long) getPrimitiveJavaObject(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObject((IonInt) o);
    }

    public long getPrimitiveJavaObject(final IonInt ionValue) {
        validateSize(ionValue);
        return ionValue.longValue();
    }
}
