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

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ionhiveserde.configuration.source.RawConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Encapsulates catalog related configuration.
 */
class CatalogConfig {

    private static final String CATALOG_PREFIX = "ion.catalog";
    private static final String IMPORTS_KEY = "ion.symbol_table_imports";

    private static final IonCatalog EMPTY_CATALOG = new SimpleCatalog();

    private static IonCatalog catalogFromReader(final IonReader reader) {
        final SimpleCatalog catalog = new SimpleCatalog();
        while (reader.next() != null) {
            @SuppressWarnings("deprecated") final SymbolTable symbolTable = _Private_Utils.newSharedSymtab(
                reader,
                true);
            catalog.putTable(symbolTable);
        }

        return catalog;
    }

    private interface CatalogLoader {

        IonCatalog loadCatalog(final String source);
    }

    private static class ClassCatalogLoader implements CatalogLoader {

        @Override
        public IonCatalog loadCatalog(final String source) {
            try {
                return (IonCatalog) Class.forName(source).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    private static class FileCatalogLoader implements CatalogLoader {

        @Override
        public IonCatalog loadCatalog(final String source) {
            try (
                final InputStream input = new FileInputStream(new File(source));
                final IonReader reader = IonReaderBuilder.standard().build(input)
            ) {
                return catalogFromReader(reader);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    private static class UrlCatalogLoader implements CatalogLoader {

        @Override
        public IonCatalog loadCatalog(final String source) {
            try (
                final InputStream input = new URL(source).openStream();
                final IonReader reader = IonReaderBuilder.standard().build(input)
            ) {
                return catalogFromReader(reader);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    enum CatalogSource {
        CLASS(new ClassCatalogLoader(), CATALOG_PREFIX + ".class"),
        FILE(new FileCatalogLoader(), CATALOG_PREFIX + ".file"),
        URL(new UrlCatalogLoader(), CATALOG_PREFIX + ".url");

        private final CatalogLoader catalogLoader;
        private final String propertyKey;

        CatalogSource(final CatalogLoader catalogLoader, final String propertyKey) {
            this.catalogLoader = catalogLoader;
            this.propertyKey = propertyKey;
        }

        public IonCatalog load(final RawConfiguration configuration) {
            return configuration.get(propertyKey)
                .map(catalogLoader::loadCatalog)
                .orElse(EMPTY_CATALOG);
        }
    }

    private final IonCatalog catalog;
    private final SymbolTable[] symbolTableImports;

    /**
     * Constructor.
     *
     * @param configuration raw configuration.
     */
    CatalogConfig(final RawConfiguration configuration) {

        catalog = Arrays.stream(CatalogSource.values())
            .filter(s -> configuration.containsKey(s.propertyKey))
            .findFirst()
            .map(s -> s.load(configuration))
            .orElse(EMPTY_CATALOG);

        final String[] importNames = configuration.get(IMPORTS_KEY)
            .map(s -> s.split(","))
            .orElse(new String[0]);

        symbolTableImports = Stream.of(importNames)
            .map(catalog::getTable)
            .toArray(SymbolTable[]::new);
    }

    /**
     * Returns the configured ion catalog.
     */
    IonCatalog getCatalog() {
        return catalog;
    }

    /**
     * Returns the symbol tables that should be imported when writing
     */
    SymbolTable[] getSymbolTableImports() {
        return symbolTableImports;
    }
}
