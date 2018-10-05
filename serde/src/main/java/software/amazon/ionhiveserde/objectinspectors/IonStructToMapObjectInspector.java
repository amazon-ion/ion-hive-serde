/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors;

import static org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonStruct} for the map<> Hive type.
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

        return struct.get(symbol.stringValue());
    }

    @Override
    public Map<?, ?> getMap(final Object data) {
        if (IonUtil.isIonNull((IonValue) data)) {
            return null;
        }

        final IonStruct struct = (IonStruct) data;

        // sets the initial size of the map to avoid growing as it's immutable while using the default HashMap load
        // factor to maintain the same collision performance
        final int size = (int) Math.ceil(struct.size() / DEFAULT_LOAD_FACTOR);

        final Map<String, IonValue> map = new HashMap<>(size, DEFAULT_LOAD_FACTOR);
        for (IonValue v : struct) {
            map.put(v.getFieldName(), v);
        }

        return map;
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
