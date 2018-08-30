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

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonValue;

/**
 * Union type object inspector. Delegates to the specific type object inspector
 */
public class IonUnionObjectInspector implements UnionObjectInspector {

    private final List<ObjectInspector> objectInspectors;

    public IonUnionObjectInspector(final List<ObjectInspector> objectInspectors) {
        this.objectInspectors = objectInspectors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ObjectInspector> getObjectInspectors() {
        return objectInspectors;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getTag(final Object o) {
        // when null assumes it's the first type of the union type
        if (isIonNull((IonValue) o)) {
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
                        continue;
                    }
                }

                default:
                    throw new IllegalStateException(
                        "Object Inspector " + objectInspector.toString() + " Not supported for object " + o.toString());
            }
        }

        throw new IllegalStateException(
            "No suitable Object Inspector found for object  " + o.toString() + " of class " + o.getClass()
                .getCanonicalName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getField(final Object o) {
        if (isIonNull((IonValue) o)) {
            return null;
        }

        return o;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTypeName() {
        return ObjectInspectorUtils.getStandardUnionTypeName(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Category getCategory() {
        return Category.UNION;
    }
}
