// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;
import java.sql.Date;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Adapts an {@link IonTimestamp} for the date Hive type.
 */
public class IonTimestampToDateObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    DateObjectInspector {

    public IonTimestampToDateObjectInspector() {
        super(TypeInfoFactory.dateTypeInfo);
    }

    @Override
    public DateWritable getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new DateWritable(getPrimitiveJavaObject((IonTimestamp) o));
    }

    @Override
    public Date getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObject((IonTimestamp) o);
    }

    private Date getPrimitiveJavaObject(final IonTimestamp ionValue) {
        return new Date(ionValue.getMillis());
    }
}

