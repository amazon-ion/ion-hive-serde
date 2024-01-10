// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration;

import com.amazon.ionhiveserde.configuration.source.RawConfiguration;

/**
 * Encapsulates Ion encoding configuration.
 */
class EncodingConfig {

    private static final String ENCODING_KEY = "ion.encoding";
    private static final String DEFAULT_ENCODING = IonEncoding.BINARY.name();
    private final IonEncoding encoding;

    /**
     * Constructor.
     *
     * @param configuration raw configuration.
     */
    EncodingConfig(final RawConfiguration configuration) {
        encoding = IonEncoding.valueOf(configuration.getOrDefault(ENCODING_KEY, DEFAULT_ENCODING));
    }

    /**
     * {@link IonEncoding} to be used when serializing, i.e. if it serializes to text or binary Ion.
     *
     * @return IonEncoding to be used.
     */
    IonEncoding getEncoding() {
        return encoding;
    }
}
