// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;

import java.io.IOException;

/**
 * Serializer for Ion blob.
 */
class BlobSerializer extends AbstractLobSerializer {

    @Override
    protected void writeValue(final IonWriter writer, final byte[] bytes) throws IOException {
        writer.writeBlob(bytes);
    }

    @Override
    public IonType getIonType() {
        return IonType.BLOB;
    }
}
