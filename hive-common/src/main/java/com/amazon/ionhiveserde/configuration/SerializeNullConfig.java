// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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
