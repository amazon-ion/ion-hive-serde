// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration;

import com.amazon.ionhiveserde.configuration.source.RawConfiguration;

/**
 * Encapsulates ignored_malformed configuration.
 */
class IgnoreMalformedConfig {

    private static final String IGNORE_MALFORMED_KEY = "ion.ignore_malformed";
    private static final String DEFAULT_IGNORE_MALFORMED = "false";

    private final boolean ignoreMalformed;

    /**
     * Constructor.
     *
     * @param configuration raw configuration
     */
    IgnoreMalformedConfig(final RawConfiguration configuration) {
        ignoreMalformed = Boolean.valueOf(
            configuration.getOrDefault(IGNORE_MALFORMED_KEY, DEFAULT_IGNORE_MALFORMED));
    }

    /**
     * Returns if should ignore malformed records instead of failing.
     */
    boolean getIgnoreMalformed() {
        return ignoreMalformed;
    }
}
