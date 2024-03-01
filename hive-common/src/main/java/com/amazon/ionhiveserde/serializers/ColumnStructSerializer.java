// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ionhiveserde.configuration.SerDeProperties;

/**
 * Struct serializer.
 */
class ColumnStructSerializer extends AbstractStructSerializer {

    ColumnStructSerializer(final IonSerializerFactory ionSerializerFactory, final SerDeProperties properties) {
        super(ionSerializerFactory, properties);
    }
}
