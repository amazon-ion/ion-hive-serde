// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonText;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Adapts an {@link IonText} for the char Hive type.
 */
public class IonTextToCharObjectInspector extends
    AbstractOverflowablePrimitiveObjectInspector<IonText, HiveChar> implements
    HiveCharObjectInspector {

    private final int maxLength;
    private final TextMaxLengthValidator validator = new TextMaxLengthValidator();

    /**
     * Creates an IonText to char with a maximum maxLength.
     *
     * @param maxLength value maxLength
     * @param failOnOverflow if fails or truncates on overflow
     */
    public IonTextToCharObjectInspector(final int maxLength, final boolean failOnOverflow) {
        super(TypeInfoFactory.getCharTypeInfo(maxLength), failOnOverflow);

        this.maxLength = maxLength;
    }

    @Override
    public HiveCharWritable getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new HiveCharWritable(getPrimitiveJavaObjectFromIonValue((IonText) o));
    }

    @Override
    public HiveChar getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObjectFromIonValue((IonText) o);
    }

    @Override
    protected HiveChar getValidatedPrimitiveJavaObject(final IonText ionValue) {
        // HiveChar truncates if necessary
        return new HiveChar(ionValue.stringValue(), maxLength);
    }

    @Override
    protected void validateSize(final IonText ionValue) {
        validator.validate(ionValue.stringValue(), maxLength);
    }
}
