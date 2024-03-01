// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;

import java.io.IOException;

/**
 * String serializer.
 */
class StringSerializer extends AbstractTextSerializer {

    @Override
    protected void writeText(final IonWriter writer, final String text) throws IOException {
        writer.writeString(text);
    }

    @Override
    public IonType getIonType() {
        return IonType.STRING;
    }
}
