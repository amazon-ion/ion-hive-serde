// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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
