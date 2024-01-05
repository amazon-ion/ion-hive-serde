/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
