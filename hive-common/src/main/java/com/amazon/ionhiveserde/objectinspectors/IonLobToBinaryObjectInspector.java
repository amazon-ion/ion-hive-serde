// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

import com.amazon.ion.IonLob;
import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * Adapts an {@link IonLob} for the binary Hive type.
 */
public class IonLobToBinaryObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    BinaryObjectInspector {

    public IonLobToBinaryObjectInspector() {
        super(TypeInfoFactory.binaryTypeInfo);
    }

    @Override
    public BytesWritable getPrimitiveWritableObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        return new BytesWritable(getPrimitiveJavaObject((IonLob) o));
    }

    @Override
    public byte[] getPrimitiveJavaObject(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObject((IonLob) o);
    }

    private byte[] getPrimitiveJavaObject(final IonLob ionValue) {
        return ionValue.getBytes();
    }
}
