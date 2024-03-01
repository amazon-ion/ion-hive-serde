// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors;

import com.amazon.ion.IonValue;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * Base class for primitive object inspectors that need to handle overflow detection.
 *
 * @param <T> Ion type.
 * @param <O> Primitive type.
 */
abstract class AbstractOverflowablePrimitiveObjectInspector<T extends IonValue, O> extends
        AbstractIonPrimitiveJavaObjectInspector {

    private final boolean failOnOverflow;

    /**
     * Constructor.
     *
     * @param typeInfo type info for this object inspector.
     * @param failOnOverflow if should fail when detecting an overflow.
     */
    AbstractOverflowablePrimitiveObjectInspector(final PrimitiveTypeInfo typeInfo,
                                                 final boolean failOnOverflow) {
        super(typeInfo);
        this.failOnOverflow = failOnOverflow;
    }

    /**
     * Gets the primitive Java representation of an Ion value detecting and handling overflows.
     *
     * @param ionValue Ion value to read as a Java primitive.
     * @return Java primitive representation.
     */
    final O getPrimitiveJavaObjectFromIonValue(final T ionValue) {
        if (failOnOverflow) {
            validateSize(ionValue);
        }

        return getValidatedPrimitiveJavaObject(ionValue);
    }

    /**
     * Gets the primitive Java representation of an Ion value that has passed overflow validation.
     *
     * @param ionValue Ion value to read as a Java primitive.
     * @return Java primitive representation.
     */
    protected abstract O getValidatedPrimitiveJavaObject(final T ionValue);

    /**
     * Validates if an ion value will overflow when converted to java primitive.
     *
     * @param ionValue Ion value to be validated.
     * @throws IllegalArgumentException when detecting an overflow.
     */
    protected abstract void validateSize(final T ionValue);
}
