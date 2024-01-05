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

package com.amazon.ionhiveserde.configuration

import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class IgnoreMalformedConfigTest {
    private fun makeConfig(encoding: String) = com.amazon.ionhiveserde.configuration.IgnoreMalformedConfig(
            MapBasedRawConfiguration(mapOf("ion.ignore_malformed" to encoding)))

    @Test
    fun ignoreMalformedTrue() {
        val subject = makeConfig("true")

        assertTrue(subject.ignoreMalformed)
    }

    @Test
    fun ignoreMalformedFalse() {
        val subject = makeConfig("false")

        assertFalse(subject.ignoreMalformed)
    }

    @Test
    fun ignoreMalformedDefault() {
        val subject = com.amazon.ionhiveserde.configuration.IgnoreMalformedConfig(MapBasedRawConfiguration(mapOf()))

        assertFalse(subject.ignoreMalformed)
    }

    @Test
    fun ignoreMalformedNotBoolean() {
        val subject = makeConfig("not a boolean")

        assertFalse(subject.ignoreMalformed)
    }
}