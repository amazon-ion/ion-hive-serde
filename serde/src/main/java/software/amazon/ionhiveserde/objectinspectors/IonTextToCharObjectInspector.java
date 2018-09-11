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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonText;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonText} for the char Hive type.
 */
public class IonTextToCharObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    HiveCharObjectInspector {

    private static final int DEFAULT_LENGTH = HiveChar.MAX_CHAR_LENGTH;

    private final int length;
    private final TextMaxLengthValidator validator = new TextMaxLengthValidator();

    public IonTextToCharObjectInspector() {
        this(DEFAULT_LENGTH);
    }

    /**
     * Creates an IonText to char with a maximum length.
     *
     * @param length max length
     */
    public IonTextToCharObjectInspector(final int length) {
        super(TypeInfoFactory.getCharTypeInfo(length));

        this.length = length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HiveCharWritable getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new HiveCharWritable(getPrimitiveJavaObject((IonText) o));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HiveChar getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObject((IonText) o);
    }

    private HiveChar getPrimitiveJavaObject(final IonText ionValue) {
        final String text = ionValue.stringValue();
        return new HiveChar(validator.validate(text, length), length);
    }
}
