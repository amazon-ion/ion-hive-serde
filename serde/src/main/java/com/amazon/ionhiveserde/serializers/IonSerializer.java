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
