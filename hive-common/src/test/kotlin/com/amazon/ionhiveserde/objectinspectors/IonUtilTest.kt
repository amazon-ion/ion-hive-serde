// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonType
import com.amazon.ionhiveserde.ION
import com.amazon.ionhiveserde.objectinspectors.IonUtil.isIonNull
import org.junit.Test
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
