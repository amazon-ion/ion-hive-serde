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

package com.amazon.ionhiveserde.configuration;

import com.amazon.ionhiveserde.configuration.source.RawConfiguration;

/**
 * Encapsulates the serialize_null configuration.
 */
class SerializeNullConfig {

    private static final String DEFAULT_SERIALIZE_NULL_KEY = "ion.serialize_null";
    private static final String DEFAULT_SERIALIZE_NULL = SerializeNullStrategy.OMIT.name();
    private final SerializeNullStrategy serializeNull;

    /**
     * Constructor.
     *
     * @param configuration raw configuration.
     */
    SerializeNullConfig(final RawConfiguration configuration) {
        serializeNull = SerializeNullStrategy.valueOf(
            configuration.getOrDefault(DEFAULT_SERIALIZE_NULL_KEY, DEFAULT_SERIALIZE_NULL));
    }

    /**
     * Returns how the serializer should write out null values.
     *
     * @return option to be used.
     */
    SerializeNullStrategy getSerializeNull() {
        return serializeNull;
    }
}
