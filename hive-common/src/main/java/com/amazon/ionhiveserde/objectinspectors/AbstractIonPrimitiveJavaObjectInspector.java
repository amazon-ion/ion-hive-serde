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

import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

/**
 * Base class for all Ion Primitive/Scalar, Object Inspectors.
 */
public abstract class AbstractIonPrimitiveJavaObjectInspector extends AbstractPrimitiveObjectInspector {

    public AbstractIonPrimitiveJavaObjectInspector(final PrimitiveTypeInfo typeInfo) {
        super(typeInfo);
    }

    @Override
    public final Object copyObject(final Object o) {
        final IonValue ionValue = (IonValue) o;
        if (isIonNull(ionValue)) {
            return null;
        }

        return ionValue.clone();
    }

    @Override
    public boolean preferWritable() {
        return false;
    }
}
