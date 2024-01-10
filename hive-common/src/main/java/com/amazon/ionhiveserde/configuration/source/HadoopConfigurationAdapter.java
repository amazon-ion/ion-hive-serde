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

package com.amazon.ionhiveserde.configuration.source;

import org.apache.hadoop.conf.Configuration;

import java.util.Optional;

/**
 * Adapts Hadoop {@link Configuration} instances to the {@link RawConfiguration} interface.
 */
public class HadoopConfigurationAdapter implements RawConfiguration {
    private final Configuration delegate;

    /**
     * Constructor.
     *
     * @param delegate hadoop configuration.
     */
    public HadoopConfigurationAdapter(final Configuration delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getOrDefault(final String key, final String defaultValue) {
        return get(key).orElse(defaultValue);
    }

    @Override
    public Optional<String> get(final String key) {
        return Optional.ofNullable(delegate.get(key));
    }

    @Override
    public boolean containsKey(final String key) {
        return delegate.get(key) != null;
    }
}
