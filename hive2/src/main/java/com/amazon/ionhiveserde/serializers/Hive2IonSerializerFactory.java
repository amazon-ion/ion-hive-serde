// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ionhiveserde.configuration.SerDeProperties;

public class Hive2IonSerializerFactory extends IonSerializerFactory {
    @Override
    protected IonSerializer newTimestampSerializer(final SerDeProperties properties) {
        return new TimestampSerializer(properties.getTimestampOffsetInMinutes());
    }
}
