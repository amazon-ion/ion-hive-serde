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

package com.amazon.ionhiveserde;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ionhiveserde.configuration.BaseProperties;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Factory for Ion reader writer and DOM factory.
 */
public class IonFactory {

    private final BaseProperties properties;

    private IonSystem domFactory;
    private IonReaderBuilder readerBuilder;
    private IonTextWriterBuilder textWriterBuilder;
    private IonBinaryWriterBuilder binaryWriterBuilder;

    public IonFactory(final BaseProperties properties) {
        this.properties = properties;
    }

    /**
     * Returns a configured DOM factory.
     * @return configured {@link IonSystem}.
     */
    public IonSystem getDomFactory() {
        if (domFactory == null) {
            domFactory = IonSystemBuilder.standard()
                .withCatalog(properties.getCatalog())
                .build();
        }

        return domFactory;
    }

    private IonReaderBuilder getReaderBuilder() {
        if (readerBuilder == null) {
            readerBuilder = IonReaderBuilder.standard()
                .withCatalog(properties.getCatalog());
        }

        return readerBuilder;
    }

    /**
     * Returns a configured IonReader.
     *
     * @param bytes byte array with ion data.
     * @param offset byte array offset to start reading the data.
     * @param length number of bytes to read from the byte array.
     * @return configured {@link IonReader}.
     */
    public IonReader newReader(final byte[] bytes, final int offset, final int length) {

        return getReaderBuilder().build(bytes, offset, length);
    }

    /**
     * Returns a configured IonReader.
     *
     * @param input reader input stream.
     * @return configured {@link IonReader}.
     */
    public IonReader newReader(final InputStream input) {
        return getReaderBuilder().build(input);
    }

    /**
     * Returns a configured text IonWriter.
     *
     * @param out writer output stream.
     * @return configured {@link IonWriter}.
     */
    public IonWriter newTextWriter(final OutputStream out) {
        if (textWriterBuilder == null) {
            textWriterBuilder = IonTextWriterBuilder.standard()
                .withCatalog(properties.getCatalog())
                .withImports(properties.getSymbolTableImports());
        }

        return textWriterBuilder.build(out);
    }

    /**
     * Returns a configured binary IonWriter.
     *
     * @param out writer output stream.
     * @return configured {@link IonWriter}.
     */
    public IonWriter newBinaryWriter(final OutputStream out) {
        if (binaryWriterBuilder == null) {
            binaryWriterBuilder = IonBinaryWriterBuilder.standard()
                .withCatalog(properties.getCatalog())
                .withImports(properties.getSymbolTableImports());
        }

        return binaryWriterBuilder.build(out);
    }
}
