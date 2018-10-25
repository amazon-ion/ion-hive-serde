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

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import software.amazon.ion.IonValue;

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
