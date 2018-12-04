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

package software.amazon.ionhiveserde;

import static software.amazon.ionhiveserde.util.SerDePropertyParser.parseOffset;

import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ionpathextraction.PathExtractor;

/**
 * Encapsulates all SerDe properties.
 */
public class SerDeProperties {

    private static final String ENCODING_KEY = "encoding";
    private static final String DEFAULT_ENCODING = IonEncoding.BINARY.name();
    private final IonEncoding encoding;

    private static final String DEFAULT_OFFSET_KEY = "timestamp.serialization_offset";
    private static final String DEFAULT_OFFSET = "Z";
    private final int timestampOffsetInMinutes;

    private static final String DEFAULT_SERIALIZE_NULL_KEY = "serialize_null";
    private static final String DEFAULT_SERIALIZE_NULL = SerializeNullStrategy.OMIT.name();
    private final SerializeNullStrategy serializeNull;

    private final FailOnOverflowConfig failOnOverflowConfig;
    private final SerializeAsConfig serializeAsConfig;
    private final PathExtractionConfig pathExtractionConfig;

    /**
     * Constructor.
     *
     * @param properties {@link Properties} passed to {@link IonHiveSerDe#initialize(Configuration, Properties)}.
     * @param columnNames table column names
     */
    SerDeProperties(final Properties properties,
                    final List<String> columnNames,
                    final List<TypeInfo> columnTypes) {
        encoding = IonEncoding.valueOf(properties.getProperty(ENCODING_KEY, DEFAULT_ENCODING));

        timestampOffsetInMinutes = parseOffset(properties.getProperty(DEFAULT_OFFSET_KEY, DEFAULT_OFFSET));

        serializeNull = SerializeNullStrategy.valueOf(
            properties.getProperty(DEFAULT_SERIALIZE_NULL_KEY, DEFAULT_SERIALIZE_NULL));

        failOnOverflowConfig = new FailOnOverflowConfig(properties, columnNames);

        serializeAsConfig = new SerializeAsConfig(properties, columnTypes);

        pathExtractionConfig = new PathExtractionConfig(properties, columnNames);
    }

    /**
     * {@link IonEncoding} to be used when serializing, i.e. if it serializes to text or binary Ion.
     *
     * @return IonEncoding to be used.
     */
    public IonEncoding getEncoding() {
        return encoding;
    }

    /**
     * Returns the timestamp timestampOffsetInMinutes in minutes to use when serializing and deserializing Ion
     * timestamps.
     *
     * @return timestamp timestampOffsetInMinutes in minutes to be used.
     */
    public int getTimestampOffsetInMinutes() {
        return timestampOffsetInMinutes;
    }

    /**
     * Returns how the serializer should write out null values.
     *
     * @return option to be used
     */
    public SerializeNullStrategy getSerializeNull() {
        return serializeNull;
    }

    /**
     * Return if the column is configured to fail when detecting an overflow.
     *
     * @return true if the column is configured to fail on overflow, false otherwise.
     */
    public boolean failOnOverflowFor(final String columnName) {
        return failOnOverflowConfig.failOnOverflowFor(columnName);
    }

    /**
     * Returns the Ion type to be used during serialization for the column.
     *
     * @return the Ion type to be used during serialization for the column.
     */
    public IonType serializationIonTypeFor(final int index) {
        return serializeAsConfig.serializationIonTypeFor(index);
    }

    /**
     * Builds a path extractor from configuration that will accumulated matched paths to the struct.
     *
     * @param struct mutable struct to accumulated matched paths.
     * @param domFactory Ion system used to create DOM objects.
     *
     * @return PathExtractor configured for matching.
     */
    public PathExtractor buildPathExtractor(final IonStruct struct, final IonSystem domFactory) {
        return pathExtractionConfig.buildPathExtractor(struct, domFactory);
    }
}

