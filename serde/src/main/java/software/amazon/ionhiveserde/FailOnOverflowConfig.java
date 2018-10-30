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

import java.util.Map;

/**
 * Encapsulates the fail_on_overflow configuration.
 */
public class FailOnOverflowConfig {

    private final Map<String, Boolean> configByColumnName;
    private final boolean defaultValue;

    /**
     * Constructor.
     *
     * @param configByColumnName user defined configuration per column name.
     * @param defaultValue default value to be used.
     */
    public FailOnOverflowConfig(final Map<String, Boolean> configByColumnName, final boolean defaultValue) {
        this.configByColumnName = configByColumnName;
        this.defaultValue = defaultValue;
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
