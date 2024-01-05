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

import java.util.*

class MapBasedRawConfiguration(private val map: Map<String, String>) : com.amazon.ionhiveserde.configuration.source.RawConfiguration {
    override fun get(key: String?): Optional<String> {
        return Optional.ofNullable(map[key])
    }

    override fun containsKey(propertyKey: String?): Boolean {
        return map.containsKey(propertyKey)
    }

    override fun getOrDefault(key: String?, defaultValue: String?): String {
        return map[key] ?: defaultValue!!
    }
}