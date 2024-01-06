/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonText;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

/**
 * Adapts an {@link IonValue} for the string Hive type. An {@link IonText} value
 * will be represented by its content, while a non-text {@link IonValue} will be
 * represented by an Ion textual representation. E.g. the Ion value
 * {@code "bar"} is an Ion string and will map to {@code "bar"}, whereas the
 * Ion value {@code { foo: "bar" }} is an Ion struct and would map to
 * something like {@code "{foo: \"bar\"}" } .
 */
public class IonValueToStringObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
        StringObjectInspector {

    public IonValueToStringObjectInspector() {
        super(TypeInfoFactory.stringTypeInfo);
    }

    @Override
    public String getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveWritableObject((IonValue) o);
    }

    @Override
    public Text getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new Text(getPrimitiveWritableObject((IonValue) o));
    }

    private String getPrimitiveWritableObject(final IonValue ionValue) {
        if (IonType.isText(ionValue.getType())) {
            return ((IonText) ionValue).stringValue();
        }
        return ionValue.toString();
    }
}
