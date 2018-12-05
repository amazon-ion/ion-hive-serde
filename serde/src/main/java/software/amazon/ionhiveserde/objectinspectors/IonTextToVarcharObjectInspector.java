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

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonText;
import software.amazon.ion.IonValue;

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
