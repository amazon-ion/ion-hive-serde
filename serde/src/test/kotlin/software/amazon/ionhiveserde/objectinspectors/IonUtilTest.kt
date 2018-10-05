/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors

import org.junit.Test
import software.amazon.ion.IonType
import software.amazon.ionhiveserde.ION
import software.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class IonUtilTest {

    @Test
    fun isIonNullForNotNull() {
        assertFalse(isIonNull(ION.newInt(1)))
    }

    @Test
    fun isIonNullForNull() {
        assertTrue(isIonNull(null))
    }

    @Test
    fun isIonNullForIonNull() {
        assertTrue(isIonNull(ION.newNull()))
    }

    @Test
    fun isIonNullForTypedNull() {
        assertTrue(isIonNull(ION.newNull(IonType.BOOL)))
    }
}