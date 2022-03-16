/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.objectinspectors.map;

import com.amazon.ionhiveserde.objectinspectors.AbstractIonPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * Base class for primitive object inspectors that need to handle overflow detection.
 *
 * @param <O> Primitive type.
 */
abstract class AbstractFieldNameObjectInspector<O> extends
        AbstractIonPrimitiveJavaObjectInspector {

    private final boolean failOnOverflow;

    /**
     * Constructor.
     *
     * @param typeInfo type info for this object inspector.
     */
    AbstractFieldNameObjectInspector(final PrimitiveTypeInfo typeInfo) {
        this(typeInfo, false);
    }

    /**
     * Constructor.
     *
     * @param typeInfo type info for this object inspector.
     * @param failOnOverflow if should fail when detecting an overflow.
     */
    AbstractFieldNameObjectInspector(final PrimitiveTypeInfo typeInfo,
                                                 final boolean failOnOverflow) {
        super(typeInfo);
        this.failOnOverflow = failOnOverflow;
    }

    /**
     * Gets the primitive Java representation of an Ion value detecting and handling overflows.
     *
     * @param fieldName Value to read as a Java primitive.
     * @return Java primitive representation.
     */
    final O getPrimitiveJavaObjectFromFieldName(final Object fieldName) {
        final String fieldNameAsString;
        if (fieldName instanceof String) {
            fieldNameAsString = (String) fieldName;
        } else {
            fieldNameAsString = fieldName.toString();
        }
        if (failOnOverflow) {
            validateSize(fieldNameAsString);
        }

        return getValidatedPrimitiveJavaObject(fieldNameAsString);
    }

    /**
     * Gets the primitive Java representation of an Ion value that has passed overflow validation.
     *
     * @param fieldName Value to read as a Java primitive.
     * @return Java primitive representation.
     */
    protected abstract O getValidatedPrimitiveJavaObject(final String fieldName);

    /**
     * Validates if an ion value will overflow when converted to java primitive.
     *
     * @param fieldName Ion value to be validated.
     * @throws IllegalArgumentException when detecting an overflow.
     * @throws UnsupportedOperationException if a function has not overridden validateSize and failOnOverflow is true.
     */
    protected void validateSize(final String fieldName) {
        if (failOnOverflow) {
            throw new UnsupportedOperationException(
                    String.format("Type %s is marked as fail on overflow, but has not implemented validation",
                            this.getClass().getSimpleName()));
        }
    }

    /**
     * Returns a java primitive object from a field name
     * @param o - Object o that contains a string version of the field name
     * @return Java primitive object
     */
    @Override
    public Object getPrimitiveJavaObject(final Object o) {
        return getPrimitiveJavaObjectFromFieldName(o.toString());
    }
}
