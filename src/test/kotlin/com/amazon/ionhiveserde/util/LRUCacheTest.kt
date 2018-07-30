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

package com.amazon.ionhiveserde.util

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class LRUCacheTest {
    val subject = LRUCache<Int, String>(3)

    @Test
    fun eldestIsLeastRecentlyUse() {
        subject[0] = "a"
        subject[1] = "b"
        subject[2] = "c"
        subject[3] = "d"

        assertEquals(3, subject.size)

        // first entry removed
        assertFalse(subject.containsKey(0))

        // others in cache
        assertEquals("b", subject[1])
        assertEquals("c", subject[2])
        assertEquals("d", subject[3])
    }

    @Test
    fun removesLeastRecentlyUsed() {
        subject[0] = "a"
        subject[1] = "b"
        subject[2] = "c"

        // using first two entries
        subject[0]
        subject[1]

        subject[3] = "d"

        assertEquals(3, subject.size)

        // least recently used entry removed
        assertFalse(subject.containsKey(2))

        // others in cache
        assertEquals("a", subject[0])
        assertEquals("b", subject[1])
        assertEquals("d", subject[3])
    }
}
