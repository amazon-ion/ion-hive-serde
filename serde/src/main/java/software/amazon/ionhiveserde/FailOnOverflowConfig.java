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

package software.amazon.ionhiveserde;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Encapsulates the fail_on_overflow configuration.
 */
class FailOnOverflowConfig {

    private static final String FAIL_ON_OVERFLOW_KEY = "fail_on_overflow";
    private static final String DEFAULT_FAIL_ON_OVERFLOW = "true";

    private final Map<String, Boolean> configByColumnName;
    private final boolean defaultValue;


    /**
     * Constructor.
     *
     * @param properties serde properties.
     * @param columnNames table column names.
     */
    FailOnOverflowConfig(final Properties properties,
                         final List<String> columnNames) {
        final String defaultValue = properties.getProperty(FAIL_ON_OVERFLOW_KEY, DEFAULT_FAIL_ON_OVERFLOW);
        configByColumnName = new HashMap<>();

        for (String columnName : columnNames) {
            final String columnPropertyKey = columnName + "." + FAIL_ON_OVERFLOW_KEY;
            final boolean columnFailOnOverflowValue = Boolean.parseBoolean(
                properties.getProperty(columnPropertyKey, defaultValue));

            configByColumnName.put(columnName, columnFailOnOverflowValue);
        }

        this.defaultValue = Boolean.valueOf(defaultValue);
    }

    /**
     * Return if the column is configured to fail when detecting an overflow.
     *
     * @return true if the column is configured to fail on overflow, false otherwise.
     */
    boolean failOnOverflowFor(final String columnName) {
        final Boolean columnValue = configByColumnName.get(columnName);
        return columnValue != null ? columnValue : defaultValue;
    }
}
