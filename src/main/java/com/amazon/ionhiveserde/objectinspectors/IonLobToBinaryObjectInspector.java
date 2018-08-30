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

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import software.amazon.ion.IonLob;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonLob} for the binary Hive type.
 */
public class IonLobToBinaryObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    BinaryObjectInspector {

    public IonLobToBinaryObjectInspector() {
        super(TypeInfoFactory.binaryTypeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BytesWritable getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        IonLob ionValue = (IonLob) o;
        return new BytesWritable(ionValue.getBytes());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        IonLob ionValue = (IonLob) o;
        return ionValue.getBytes();
    }
}
