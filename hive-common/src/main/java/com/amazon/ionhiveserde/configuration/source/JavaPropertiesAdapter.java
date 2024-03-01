// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration.source;

import java.util.Optional;
import java.util.Properties;

/**
 * Adapts Java {@link Properties} instances to the {@link RawConfiguration} interface.
 */
public class JavaPropertiesAdapter implements RawConfiguration {
    private final Properties delegate;

    /**
     * Constructor.
     *
     * @param delegate java properties to adapt.
     */
    public JavaPropertiesAdapter(final Properties delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getOrDefault(final String key, final String defaultValue) {
        return get(key).orElse(defaultValue);
    }

    @Override
    public Optional<String> get(final String key) {
        return Optional.ofNullable(delegate.getProperty(key));
    }

    @Override
    public boolean containsKey(final String key) {
        return delegate.containsKey(key);
    }
}
