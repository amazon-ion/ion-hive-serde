// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import static com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

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

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
        return fields;
    }

    @Override
    public StructField getStructFieldRef(final String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("field name cannot be null");
        }

        return fieldsByName.get(fieldName);
    }

    @Override
    public Object getStructFieldData(final Object data, final StructField fieldRef) {
        if (isIonNull((IonValue) data)) {
            return null;
        }
        if (fieldRef == null) {
            throw new IllegalArgumentException("fieldRef name cannot be null");
        }

        final IonStruct struct = (IonStruct) data;

        return IonUtil.handleNull(struct.get(fieldRef.getFieldName()));
    }

    @Override
    public List<Object> getStructFieldsDataAsList(final Object data) {
        if (isIonNull((IonValue) data)) {
            return null;
        }

        final IonStruct struct = (IonStruct) data;

        return StreamSupport.stream(struct.spliterator(), false).map(IonUtil::handleNull)
                .collect(Collectors.toList());
    }

    @Override
    public String getTypeName() {
        return ObjectInspectorUtils.getStandardStructTypeName(this);
    }

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

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public ObjectInspector getFieldObjectInspector() {
            return fieldObjectInspector;
        }

        @Override
        public int getFieldID() {
            return fieldId;
        }

        @Override
        public String getFieldComment() {
            return FIELD_COMMENT;
        }
    }
}

