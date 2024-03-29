// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import static org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonValue;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Adapts an {@link IonStruct} for the map Hive type.
 */
public class IonStructToMapObjectInspector implements MapObjectInspector {

    // from java.util.HashMap
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private final ObjectInspector keyObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    private final ObjectInspector valueObjectInspector;

    public IonStructToMapObjectInspector(final ObjectInspector valueObjectInspector) {
        this.valueObjectInspector = valueObjectInspector;
    }

    @Override
    public ObjectInspector getMapKeyObjectInspector() {
        return keyObjectInspector;
    }

    @Override
    public ObjectInspector getMapValueObjectInspector() {
        return valueObjectInspector;
    }

    @Override
    public Object getMapValueElement(final Object data, final Object key) {
        if (IonUtil.isIonNull((IonValue) data)) {
            return null;
        }
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        final IonStruct struct = (IonStruct) data;
        final IonSymbol symbol = (IonSymbol) key;

        return IonUtil.handleNull(struct.get(symbol.stringValue()));
    }

    @Override
    public Map<?, ?> getMap(final Object data) {
        if (IonUtil.isIonNull((IonValue) data)) {
            return null;
        }

        final IonStruct struct = (IonStruct) data;

        return StreamSupport.stream(struct.spliterator(), false)
                .collect(HashMap::new, (m,v) -> m.put(v.getFieldName(), IonUtil.handleNull(v)), HashMap::putAll);
    }

    @Override
    public int getMapSize(final Object data) {
        if (IonUtil.isIonNull((IonValue) data)) {
            return -1;
        }

        final IonStruct struct = (IonStruct) data;

        return struct.size();
    }

    @Override

    public String getTypeName() {
        return MAP_TYPE_NAME + "<" + keyObjectInspector.getTypeName() + "," + valueObjectInspector.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
        return Category.MAP;
    }
}
