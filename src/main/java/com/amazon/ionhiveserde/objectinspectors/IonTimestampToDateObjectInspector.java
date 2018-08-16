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

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonValue;

import java.sql.Date;

/**
 * Object Inspector for {@link software.amazon.ion.IonText} to {@link Date}
 */
public class IonTimestampToDateObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements DateObjectInspector {

    IonTimestampToDateObjectInspector() {
        super(TypeInfoFactory.dateTypeInfo);
    }

    @Override
    public DateWritable getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonTimestamp ionValue = (IonTimestamp) o;
        return new DateWritable(new Date(ionValue.getMillis()));
    }

    @Override
    public Date getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) return null;

        IonTimestamp ionValue = (IonTimestamp) o;
        return new Date(ionValue.getMillis());
    }
}

