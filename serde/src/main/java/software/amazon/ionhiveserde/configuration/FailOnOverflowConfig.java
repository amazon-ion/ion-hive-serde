/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.ionhiveserde.configuration.source.RawConfiguration;

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
