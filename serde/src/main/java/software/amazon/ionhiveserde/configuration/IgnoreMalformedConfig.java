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

import software.amazon.ionhiveserde.configuration.source.RawConfiguration;

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
