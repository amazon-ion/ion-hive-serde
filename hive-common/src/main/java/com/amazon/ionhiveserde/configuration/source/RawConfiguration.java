// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration.source;

import java.util.Optional;

/**
 * Raw configuration as a mapping from string keys to string values.
 */
public interface RawConfiguration {

    /**
     * Gets the configured value for a key. If no value is configured returns the default value.
     * @param key key to lookup.
     * @param defaultValue value to return if no value is associated to the key.
     * @return found value or defaultValue.
     */
    String getOrDefault(final String key, final String defaultValue);

    /**
     * Gets the configured value for a key.
     *
     * @param key key to lookup.
     * @return found value or Optional.empty()
     */
    Optional<String> get(final String key);

    /**
     * Returns true if there is a value configured for the key
     *
     * @param key key to lookup .
     * @return true if it has a value associated with the key.
     */
    boolean containsKey(final String key);
}

