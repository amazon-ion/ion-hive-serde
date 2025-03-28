// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

import com.amazon.ion.IonNumber;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Adapts an {@link IonNumber} for the decimal Hive type.
 */
public class IonNumberToDecimalObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    HiveDecimalObjectInspector {

    public IonNumberToDecimalObjectInspector() {

        super(TypeInfoFactory.decimalTypeInfo);
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        return new HiveDecimalWritable(getPrimitiveJavaObject(o));
    }

    @Override
    public HiveDecimal getPrimitiveJavaObject(final Object o) {
        final IonValue value = (IonValue) o;
        if (isIonNull(value)) {
            return null;
        }

        if (value instanceof IonNumber) {
            return HiveDecimal.create(((IonNumber) value).bigDecimalValue());
        } else {
            throw new IllegalArgumentException("Invalid Ion type: " + value.getType());
        }
    }
}
