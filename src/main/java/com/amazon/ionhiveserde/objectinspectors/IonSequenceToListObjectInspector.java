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
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME;

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonSequence} for the array<> Hive type.
 */
public class IonSequenceToListObjectInspector implements ListObjectInspector {

    private final ObjectInspector elementObjectInspector;

    public IonSequenceToListObjectInspector(final ObjectInspector elementObjectInspector) {
        this.elementObjectInspector = elementObjectInspector;
    }

    /**
     * Elements object inspectors.
     */
    @Override
    public ObjectInspector getListElementObjectInspector() {
        return elementObjectInspector;
    }

    @Override
    public Object getListElement(final Object data, final int index) {
        if (isIonNull((IonValue) data)) {
            return null;
        }

        final IonSequence sequence = (IonSequence) data;
        if (index < 0 || sequence.size() <= index) {
            return null;
        }

        return sequence.get(index);
    }

    @Override
    public int getListLength(final Object data) {
        if (isIonNull((IonValue) data)) {
            return -1;
        }

        final IonSequence sequence = (IonSequence) data;
        return sequence.size();
    }

    @Override
    public List<?> getList(final Object data) {
        if (isIonNull((IonValue) data)) {
            return null;
        }

        return (IonSequence) data;
    }

    @Override
    public String getTypeName() {
        return LIST_TYPE_NAME + "<" + elementObjectInspector.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
        return Category.LIST;
    }
}
