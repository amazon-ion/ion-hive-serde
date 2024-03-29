// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;

/**
 * Union type object inspector. Delegates to the specific type object inspector
 */
public class IonUnionObjectInspector implements UnionObjectInspector {

    private final List<ObjectInspector> objectInspectors;

    public IonUnionObjectInspector(final List<ObjectInspector> objectInspectors) {
        this.objectInspectors = objectInspectors;
    }

    @Override
    public List<ObjectInspector> getObjectInspectors() {
        return objectInspectors;
    }

    @Override
    public byte getTag(final Object o) {
        // when null assumes it's the first type of the union type
        if (IonUtil.isIonNull((IonValue) o)) {
            return 0;
        }

        for (byte i = 0; i < objectInspectors.size(); i++) {
            final ObjectInspector objectInspector = objectInspectors.get(i);
            switch (objectInspector.getCategory()) {
                case LIST:
                    if (o instanceof IonSequence) {
                        return i;
                    }
                    break;

                case STRUCT:
                case MAP:
                    if (o instanceof IonStruct) {
                        return i;
                    }
                    break;

                case UNION:
                    return i;

                case PRIMITIVE: {
                    final PrimitiveObjectInspector primitiveObjectInspector =
                        (PrimitiveObjectInspector) objectInspector;
                    try {
                        // try to handle it, return the ObjectInspector tag if able to
                        primitiveObjectInspector.getPrimitiveJavaObject(o);
                        return i;
                    } catch (Exception ex) {
                        // goes to next ObjectInspector
                    }
                }
            }
        }

        throw new IllegalArgumentException(
            "No suitable Object Inspector found for object  " + o.toString() + " of class " + o.getClass()
                .getCanonicalName());
    }

    @Override
    public Object getField(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return o;
    }

    @Override
    public String getTypeName() {
        return ObjectInspectorUtils.getStandardUnionTypeName(this);
    }

    @Override
    public Category getCategory() {
        return Category.UNION;
    }
}
