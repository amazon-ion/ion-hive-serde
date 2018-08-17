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

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonText;
import software.amazon.ion.IonValue;

/**
 * Object Inspector for {@link software.amazon.ion.IonText} to {@link HiveVarchar}
 */
public class IonTextToVarcharObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements HiveVarcharObjectInspector {
    private static final int DEFAULT_LENGTH = HiveVarchar.MAX_VARCHAR_LENGTH;

    private final int length;
    private final TextMaxLengthValidator validator = new TextMaxLengthValidator();

    IonTextToVarcharObjectInspector() {
        this(DEFAULT_LENGTH);
    }

    IonTextToVarcharObjectInspector(final int length) {
        super(TypeInfoFactory.getVarcharTypeInfo(length));

        this.length = length;
    }

    @Override
    public HiveVarcharWritable getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonText ionValue = (IonText) o;
        String text = ionValue.stringValue();
        validator.validate(text, length);
        HiveVarchar hiveVarchar = new HiveVarchar(validator.validate(text, length), length);
        return new HiveVarcharWritable(hiveVarchar);
    }

    @Override
    public HiveVarchar getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonText ionValue = (IonText) o;
        String text = ionValue.stringValue();
        validator.validate(text, length);
        return new HiveVarchar(validator.validate(text, length), length);
    }
}
