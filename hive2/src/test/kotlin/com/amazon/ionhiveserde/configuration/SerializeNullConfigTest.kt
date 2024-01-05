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

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class SerializeNullConfigTest {
    private fun makeConfig(serializeNull: String) = com.amazon.ionhiveserde.configuration.SerializeNullConfig(
            MapBasedRawConfiguration(mapOf("ion.serialize_null" to serializeNull)))

    private fun serializeNullOptions() = com.amazon.ionhiveserde.configuration.SerializeNullStrategy.values()

    @Test
    @Parameters(method = "serializeNullOptions")
    fun serializeNull(serializeNullStrategy: com.amazon.ionhiveserde.configuration.SerializeNullStrategy) {
        val subject = makeConfig(serializeNullStrategy.name)

        assertEquals(serializeNullStrategy, subject.serializeNull)
    }

    @Test
    fun defaultSerializeNull() {
        val subject = com.amazon.ionhiveserde.configuration.SerializeNullConfig(MapBasedRawConfiguration(mapOf()))

        assertEquals(com.amazon.ionhiveserde.configuration.SerializeNullStrategy.OMIT, subject.serializeNull)
    }

    @Test(expected = IllegalArgumentException::class)
    fun invalidSerializeNull() {
        makeConfig("invalid option")
    }
}