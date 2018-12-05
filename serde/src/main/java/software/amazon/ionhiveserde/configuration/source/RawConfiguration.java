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

package software.amazon.ionhiveserde.configuration.source;

import java.util.Optional;

/**
 * Raw configuration as a mapping from string keys to string values.
 */
public interface RawConfiguration {

    /**
     * Gets the configured value for a key. If no value is configured returns the default value.
     */
    String getOrDefault(final String key, final String defaultValue);

    /**
     * Gets the configured value for a key.
     */
    Optional<String> get(final String key);

    /**
     * Returns true if there is a value configured for the key
     */
    boolean containsKey(final String key);
}

