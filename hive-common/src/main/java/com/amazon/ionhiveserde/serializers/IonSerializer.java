// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import java.io.IOException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Interface for Ion serializers.
 */
interface IonSerializer {

    /**
     * Serializes an Ion value to writer by extracting information from data using the object inspector.
     *
     * @param writer writer to use.
     * @param data data to serialize.
     * @param objectInspector object inspector to analyze data.
     * @throws IOException when writer is not able to write out a value .
     * @throws IllegalArgumentException if serializer is not able to handle the data.
     */
    void serialize(final IonWriter writer,
                   final Object data,
                   final ObjectInspector objectInspector) throws IOException;

    /**
     * Ion type handled by this serializer.
     *
     * @return type handled by this serializer.
     */
    IonType getIonType();
}
