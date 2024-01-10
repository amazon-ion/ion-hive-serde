// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonText;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

/**
 * Adapts an {@link IonText} for the string Hive type.
 */
public class IonTextToStringObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    StringObjectInspector {

    public IonTextToStringObjectInspector() {
        super(TypeInfoFactory.stringTypeInfo);
    }

    @Override
    public String getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveWritableObject((IonText) o);
    }

    @Override
    public Text getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new Text(getPrimitiveWritableObject((IonText) o));
    }

    private String getPrimitiveWritableObject(final IonText ionValue) {
        return ionValue.stringValue();
    }
}
