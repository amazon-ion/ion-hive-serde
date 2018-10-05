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

import static software.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import software.amazon.ion.IonBool;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonBool} for the boolean Hive type.
 */
public class IonBooleanToBooleanObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    BooleanObjectInspector {

    public IonBooleanToBooleanObjectInspector() {
        super(TypeInfoFactory.booleanTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        return new BooleanWritable(getPrimitiveJavaObject((IonBool) o));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean get(final Object o) {
        return (boolean) getPrimitiveJavaObject(o);
    }

    /**
     * {@inheritDoc}
     */
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


