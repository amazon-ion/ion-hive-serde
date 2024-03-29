// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME;

import com.amazon.ion.IonSequence;
import com.amazon.ion.IonValue;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Adapts an {@link IonSequence} for the array Hive type.
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

        return IonUtil.handleNull(sequence.get(index));
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

        final IonSequence sequence = (IonSequence) data;
        return sequence.stream().map(IonUtil::handleNull).collect(Collectors.toList());
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
