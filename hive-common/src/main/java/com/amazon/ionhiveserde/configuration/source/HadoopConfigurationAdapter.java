// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration.source;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;

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
