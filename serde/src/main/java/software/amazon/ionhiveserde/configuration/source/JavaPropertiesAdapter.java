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
