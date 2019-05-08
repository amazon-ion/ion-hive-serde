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

package com.amazon.ionhiveserde.configuration;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.SymbolTable;
import com.amazon.ionhiveserde.configuration.source.RawConfiguration;

/**
 * Base class for configuration properties.
 */
public abstract class BaseProperties {

    private final CatalogConfig catalogConfig;
    private final IgnoreMalformedConfig ignoreMalformedConfig;

    BaseProperties(final RawConfiguration configuration) {
        catalogConfig = new CatalogConfig(configuration);
        ignoreMalformedConfig = new IgnoreMalformedConfig(configuration);
    }

    /**
     * @see CatalogConfig#getCatalog()
     * @return configured catalog.
     */
    public IonCatalog getCatalog() {
        return catalogConfig.getCatalog();
    }

    /**
     * @see CatalogConfig#getSymbolTableImports()
     * @return configured catalog.
     */
    public SymbolTable[] getSymbolTableImports() {
        return catalogConfig.getSymbolTableImports();
    }

    /**
     * @see IgnoreMalformedConfig#getIgnoreMalformed()
     * @return configured catalog.
     */
    public boolean getIgnoreMalformed() {
        return ignoreMalformedConfig.getIgnoreMalformed();
    }
}
