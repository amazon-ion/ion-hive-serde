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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonText;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonText} for the char Hive type
 */
public class IonTextToCharObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements HiveCharObjectInspector {

    private static final int DEFAULT_LENGTH = HiveChar.MAX_CHAR_LENGTH;

    private final int length;
    private final TextMaxLengthValidator validator = new TextMaxLengthValidator();

    IonTextToCharObjectInspector() {
        this(DEFAULT_LENGTH);
    }

    IonTextToCharObjectInspector(final int length) {
        super(TypeInfoFactory.getCharTypeInfo(length));

        this.length = length;
    }

    @Override
    public HiveCharWritable getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonText ionValue = (IonText) o;
        String text = ionValue.stringValue();
        validator.validate(text, length);

        HiveChar hiveVarchar = new HiveChar(validator.validate(text, length), length);
        return new HiveCharWritable(hiveVarchar);
    }

    @Override
    public HiveChar getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonText ionValue = (IonText) o;
        String text = ionValue.stringValue();
        validator.validate(text, length);

        return new HiveChar(validator.validate(text, length), length);
    }
}
