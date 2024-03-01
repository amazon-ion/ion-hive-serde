// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;

import java.io.IOException;

/**
 * Serializer for clob.
 */
class ClobSerializer extends AbstractLobSerializer {

    @Override
    public IonType getIonType() {
        return IonType.CLOB;
    }

    @Override
    protected void writeValue(final IonWriter writer, final byte[] bytes) throws IOException {
        writer.writeClob(bytes);
    }
}
