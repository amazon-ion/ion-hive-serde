// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;

import java.io.IOException;

/**
 * Symbol serializer.
 */
class SymbolSerializer extends AbstractTextSerializer {
    @Override
    public IonType getIonType() {
        return IonType.SYMBOL;
    }

    @Override
    protected void writeText(final IonWriter writer, final String text) throws IOException {
        writer.writeSymbol(text);
    }
}
