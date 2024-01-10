// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonText;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Adapts an {@link IonText} for the varchar Hive type.
 */
public class IonTextToVarcharObjectInspector extends
    AbstractOverflowablePrimitiveObjectInspector<IonText, HiveVarchar> implements
    HiveVarcharObjectInspector {

    private final int maxLength;
    private final TextMaxLengthValidator validator = new TextMaxLengthValidator();

    /**
     * Creates an IonText to varchar with a maximum maxLength.
     *
     * @param maxLength max maxLength
     * @param failOnOverflow if fails or truncates on overflow
     */
    public IonTextToVarcharObjectInspector(final int maxLength, final boolean failOnOverflow) {
        super(TypeInfoFactory.getVarcharTypeInfo(maxLength), failOnOverflow);

        this.maxLength = maxLength;
    }

    @Override
    public HiveVarcharWritable getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new HiveVarcharWritable(getPrimitiveJavaObjectFromIonValue((IonText) o));
    }

    @Override
    public HiveVarchar getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObjectFromIonValue((IonText) o);
    }

    @Override
    protected HiveVarchar getValidatedPrimitiveJavaObject(final IonText ionValue) {
        return new HiveVarchar(ionValue.stringValue(), maxLength);
    }

    @Override
    protected void validateSize(final IonText ionValue) {
        validator.validate(ionValue.stringValue(), maxLength);
    }
}
