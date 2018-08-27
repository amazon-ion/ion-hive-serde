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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import software.amazon.ion.IonText;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonText} for the string Hive type
 */
public class IonTextToStringObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements StringObjectInspector {

    IonTextToStringObjectInspector() {
        super(TypeInfoFactory.stringTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonText ionValue = (IonText) o;
        return ionValue.stringValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Text getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonText ionValue = (IonText) o;
        return new Text(ionValue.stringValue());
    }
}
