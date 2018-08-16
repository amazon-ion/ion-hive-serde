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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import software.amazon.ion.IonStruct;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Placeholder implementation for structs, just enough to allow for simple roundtrip tests.
 * <p>
 * FIXME make it package private
 */
public class IonStructObjectInspector extends StructObjectInspector {

    private final Map<String, IonStructField> fieldsByName;
    private final List<IonStructField> fields;

    public IonStructObjectInspector(StructTypeInfo tableInfo) {
        // FIXME move some of this to a factory to cache it

        fieldsByName = new HashMap<>();
        fields = new ArrayList<>();

        for (int i = 0; i < tableInfo.getAllStructFieldNames().size(); i++) {
            String fieldName = tableInfo.getAllStructFieldNames().get(i);
            TypeInfo typeInfo = tableInfo.getAllStructFieldTypeInfos().get(i);

            ObjectInspector objectInspector = IonObjectInspectorFactory.objectInspectorFor(typeInfo);
            IonStructField field = new IonStructField(fieldName, objectInspector, i, "");

            fields.add(field);
            fieldsByName.put(fieldName, field);
        }
    }

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
        return fields;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
        return fieldsByName.get(fieldName);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        IonStruct struct = (IonStruct) data;

        return struct.get(fieldRef.getFieldName());
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
        if (data == null) {
            return null;
        }
        // We support both List<Object> and Object[]
        // so we have to do differently.
        if (!(data instanceof List)) {
            data = Arrays.asList((Object[]) data);
        }

        List<Object> list = (List<Object>) data;
        assert (list.size() == fields.size());
        return list;
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

        private final String fieldName;
        private final ObjectInspector fieldObjectInspector;
        private final int fieldID;
        private final String fieldComment; // FIXME what is this?

        IonStructField(String fieldName, ObjectInspector fieldObjectInspector, int fieldID, String fieldComment) {
            this.fieldName = fieldName;
            this.fieldObjectInspector = fieldObjectInspector;
            this.fieldID = fieldID;
            this.fieldComment = fieldComment;
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
            return fieldID;
        }

        @Override
        public String getFieldComment() {
            return fieldComment;
        }
    }
}

