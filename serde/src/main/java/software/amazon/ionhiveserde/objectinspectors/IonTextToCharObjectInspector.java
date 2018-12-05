/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonText;
import software.amazon.ion.IonValue;

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
