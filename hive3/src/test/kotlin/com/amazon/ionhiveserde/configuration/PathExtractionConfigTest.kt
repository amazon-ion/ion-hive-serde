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

import com.amazon.ion.IonInt
import com.amazon.ion.IonSequence
import com.amazon.ion.IonStruct
import com.amazon.ion.system.IonReaderBuilder
import com.amazon.ionhiveserde.ION
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonSequenceCaseInsensitiveDecorator
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonStructCaseInsensitiveDecorator
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PathExtractionConfigTest {
    @Test
    fun pathExtractor() {
        val ionDocument = "{f1: 1, obj: {f2: 2}}"

        val struct = ION.newEmptyStruct()

        val configMap = mapOf(
                "ion.c1.path_extractor" to "(f1)",
                "ion.c2.path_extractor" to "(obj f2)")

        val pathExtractor = com.amazon.ionhiveserde.configuration.PathExtractionConfig(
                MapBasedRawConfiguration(configMap),
                listOf("c1", "c2")
        ).pathExtractor()

        assertTrue(struct.isEmpty)

        pathExtractor.match(IonReaderBuilder.standard().build(ionDocument), struct)

        assertEquals(2, struct.size())
        assertEquals(1, (struct["c1"] as IonInt).intValue())
        assertEquals(2, (struct["c2"] as IonInt).intValue())
    }

    @Test
    fun caseInsensitiveNestedStruct() {
        val ionDocument = "{f1: [{Foo: 2, foo:3}]}"

        val rawStruct: IonStruct = ION.newEmptyStruct()
        val struct = IonStructCaseInsensitiveDecorator(rawStruct)

        val configMap = mapOf(
                "ion.path_extractor.case_sensitive" to "false",
                "ion.c1.path_extractor" to "(f1)",
        )

        val pathExtractor = PathExtractionConfig(
                MapBasedRawConfiguration(configMap),
                listOf("c1")
        ).pathExtractor()

        assertTrue(struct.isEmpty)

        pathExtractor.match(IonReaderBuilder.standard().build(ionDocument), struct)

        assertTrue(struct.get("c1") is IonSequenceCaseInsensitiveDecorator)
        assertTrue((struct.get("c1") as IonSequence)[0] is IonStructCaseInsensitiveDecorator)
    }

    @Test
    fun caseInsensitiveNestedSequence() {
        val ionDocument = "{f1: [{Foo: bar}]}"

        val rawStruct: IonStruct = ION.newEmptyStruct()
        val struct = IonStructCaseInsensitiveDecorator(rawStruct)

        val configMap = mapOf(
                "ion.path_extractor.case_sensitive" to "false",
                "ion.c1.path_extractor" to "(f1)",
        )

        val pathExtractor = PathExtractionConfig(
                MapBasedRawConfiguration(configMap),
                listOf("c1")
        ).pathExtractor()

        assertTrue(struct.isEmpty)

        pathExtractor.match(IonReaderBuilder.standard().build(ionDocument), struct)

        assertTrue(struct.get("c1") is IonSequenceCaseInsensitiveDecorator)
        assertTrue((struct.get("c1") as IonSequence)[0] is IonStructCaseInsensitiveDecorator)
    }
}
