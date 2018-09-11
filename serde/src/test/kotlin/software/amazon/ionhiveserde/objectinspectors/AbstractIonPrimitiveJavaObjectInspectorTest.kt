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

package software.amazon.ionhiveserde.objectinspectors

import org.junit.Test
import software.amazon.ionhiveserde.ionNull
import kotlin.test.assertNull

/**
 * Base class for ObjectInspector unit tests with common helper methods
 */
abstract class AbstractIonPrimitiveJavaObjectInspectorTest {
    protected abstract val subject: AbstractIonPrimitiveJavaObjectInspector

    @Test
    fun getPrimitiveWritableObjectForNull() {
        assertNull(subject.getPrimitiveWritableObject(null))
    }

    @Test
    fun getPrimitiveWritableObjectForIonNull() {
        assertNull(subject.getPrimitiveWritableObject(ionNull))
    }

    @Test
    fun getPrimitiveJavaObjectForNull() {
        assertNull(subject.getPrimitiveJavaObject(null))
    }

    @Test
    fun getPrimitiveJavaObjectForIonNull() {
        assertNull(subject.getPrimitiveJavaObject(ionNull))
    }
}

