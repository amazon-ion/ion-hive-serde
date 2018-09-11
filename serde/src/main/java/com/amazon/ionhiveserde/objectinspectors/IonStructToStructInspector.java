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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonStruct} for the struct Hive type.
 */
public class IonStructToStructInspector extends StructObjectInspector {

    private final Map<String, IonStructField> fieldsByName;
    private final List<IonStructField> fields;

    /**
     * Creates a Ion struct to Hive struct object inspector.
     *
     * @param structTypeInfo TypeInfo for all fields
     * @param fieldObjectInspectors list of field ObjectInspectors in order
     */
    public IonStructToStructInspector(final StructTypeInfo structTypeInfo,
        final List<ObjectInspector> fieldObjectInspectors) {
        fieldsByName = new HashMap<>();
        fields = new ArrayList<>();

        final ArrayList<String> structFieldNames = structTypeInfo.getAllStructFieldNames();
        for (int i = 0; i < structFieldNames.size(); i++) {
            final String fieldName = structFieldNames.get(i);
            final ObjectInspector objectInspector = fieldObjectInspectors.get(i);

            final IonStructField field = new IonStructField(fieldName, objectInspector, i);
            fields.add(field);
            fieldsByName.put(fieldName, field);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
        return fields;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StructField getStructFieldRef(final String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("field name cannot be null");
        }

        return fieldsByName.get(fieldName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getStructFieldData(final Object data, final StructField fieldRef) {
        if (isIonNull((IonValue) data)) {
            return null;
        }
        if (fieldRef == null) {
            throw new IllegalArgumentException("fieldRef name cannot be null");
        }

        final IonStruct struct = (IonStruct) data;

        return struct.get(fieldRef.getFieldName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Object> getStructFieldsDataAsList(final Object data) {
        if (isIonNull((IonValue) data)) {
            return null;
        }

        final IonStruct struct = (IonStruct) data;

        List<Object> list = new ArrayList<>(struct.size());
        for (IonValue v : struct) {
            list.add(v);
        }

        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTypeName() {
        return ObjectInspectorUtils.getStandardStructTypeName(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Category getCategory() {
        return Category.STRUCT;
    }

    private static class IonStructField implements StructField {

        private static final String FIELD_COMMENT = "";
        private final String fieldName;
        private final ObjectInspector fieldObjectInspector;
        private final int fieldId;

        IonStructField(final String fieldName, final ObjectInspector fieldObjectInspector, final int fieldId) {
            this.fieldName = fieldName;
            this.fieldObjectInspector = fieldObjectInspector;
            this.fieldId = fieldId;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getFieldName() {
            return fieldName;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ObjectInspector getFieldObjectInspector() {
            return fieldObjectInspector;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getFieldID() {
            return fieldId;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getFieldComment() {
            return FIELD_COMMENT;
        }
    }
}

