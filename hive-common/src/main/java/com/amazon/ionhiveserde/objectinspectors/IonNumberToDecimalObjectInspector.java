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

import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

/**
 * Adapts an {@link IonDecimal} or {@link IonInt} for the decimal Hive type.
 */
public class IonNumberToDecimalObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    HiveDecimalObjectInspector {

    public IonNumberToDecimalObjectInspector() {

        super(TypeInfoFactory.decimalTypeInfo);
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        return new HiveDecimalWritable(getPrimitiveJavaObject(o));
    }

    @Override
    public HiveDecimal getPrimitiveJavaObject(final Object o) {
        final IonValue value = (IonValue) o;
        if (isIonNull((IonValue) o)) {
            return null;
        }

        switch (value.getType()) {
            case INT:
                return getPrimitiveJavaObject((IonInt) o);

            case DECIMAL:
                return getPrimitiveJavaObject((IonDecimal) o);

            default:
                throw new IllegalArgumentException("Invalid Ion type: " + value.getType());
        }
    }

    private HiveDecimal getPrimitiveJavaObject(final IonDecimal ionValue) {
        return HiveDecimal.create(ionValue.bigDecimalValue());
    }

    private HiveDecimal getPrimitiveJavaObject(final IonInt ionValue) {
        return HiveDecimal.create(ionValue.bigIntegerValue());
    }
}
