// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration;

import com.amazon.ionhiveserde.configuration.source.RawConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates the fail_on_overflow configuration.
 */
class FailOnOverflowConfig {

    private static final String FAIL_ON_OVERFLOW_KEY_FORMAT = "ion.%s.fail_on_overflow";
    private static final String DEFAULT_FAIL_ON_KEY = "ion.fail_on_overflow";
    private static final String DEFAULT_FAIL_ON_OVERFLOW = "true";

    private final Map<String, Boolean> configByColumnName;
    private final boolean defaultValue;


    /**
     * Constructor.
     *
     * @param configuration raw configuration source.
     * @param columnNames table column names.
     */
    FailOnOverflowConfig(final RawConfiguration configuration, final List<String> columnNames) {
        configByColumnName = new HashMap<>();

        final String defaultValue = configuration.getOrDefault(DEFAULT_FAIL_ON_KEY, DEFAULT_FAIL_ON_OVERFLOW);
        this.defaultValue = Boolean.valueOf(defaultValue);

        for (String columnName : columnNames) {
            final String columnFailOnOverflowValue = configuration.getOrDefault(
                String.format(FAIL_ON_OVERFLOW_KEY_FORMAT, columnName),
                defaultValue);

            configByColumnName.put(columnName, Boolean.valueOf(columnFailOnOverflowValue));
        }
    }

    /**
     * Return if the column is configured to fail when detecting an overflow.
     *
     * @return true if the column is configured to fail on overflow, false otherwise.
     */
    boolean failOnOverflowFor(final String columnName) {
        return configByColumnName.getOrDefault(columnName, defaultValue);
    }
}
