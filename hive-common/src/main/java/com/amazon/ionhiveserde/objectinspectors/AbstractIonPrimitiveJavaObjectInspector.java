// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * Base class for all Ion Primitive/Scalar, Object Inspectors.
 */
public abstract class AbstractIonPrimitiveJavaObjectInspector extends AbstractPrimitiveObjectInspector {

    public AbstractIonPrimitiveJavaObjectInspector(final PrimitiveTypeInfo typeInfo) {
        super(typeInfo);
    }

    @Override
    public final Object copyObject(final Object o) {
        final IonValue ionValue = (IonValue) o;
        if (isIonNull(ionValue)) {
            return null;
        }

        return ionValue.clone();
    }

    @Override
    public boolean preferWritable() {
        return false;
    }
}
