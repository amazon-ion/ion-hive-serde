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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;

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

    private static final String FAIL_ON_OVERFLOW_KEY = "fail_on_overflow";
    private static final String FAIL_ON_OVERFLOW = "true";
    private final FailOnOverflowConfig failOnOverflowConfig;

    /**
     * Constructor.
     *
     * @param properties {@link Properties} passed to {@link IonHiveSerDe#initialize(Configuration, Properties)}.
     * @param columnNames table column names
     */
    SerDeProperties(final Properties properties, final List<String> columnNames) {
        encoding = IonEncoding.valueOf(properties.getProperty(ENCODING_KEY, DEFAULT_ENCODING));
        timestampOffsetInMinutes = parseOffset(properties.getProperty(DEFAULT_OFFSET_KEY, DEFAULT_OFFSET));
        serializeNull = SerializeNullStrategy.valueOf(
            properties.getProperty(DEFAULT_SERIALIZE_NULL_KEY, DEFAULT_SERIALIZE_NULL));
        failOnOverflowConfig = parseFailOnOverflow(properties, columnNames);
    }

    /**
     * {@link IonEncoding} to be used when serializing, i.e. if it serializes to text or binary Ion.
     *
     * @return IonEncoding to be used.
     */
    IonEncoding getEncoding() {
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

    private FailOnOverflowConfig parseFailOnOverflow(final Properties properties,
                                                     final List<String> columnNames) {
        final String defaultValue = properties.getProperty(FAIL_ON_OVERFLOW_KEY, FAIL_ON_OVERFLOW);
        final Map<String, Boolean> configByColumnName = new HashMap<>();

        for (String columnName : columnNames) {
            final String columnPropertyKey = columnName + "." + FAIL_ON_OVERFLOW_KEY;
            final boolean columnFailOnOverflowValue = Boolean.parseBoolean(
                properties.getProperty(columnPropertyKey, defaultValue));

            configByColumnName.put(columnName, columnFailOnOverflowValue);
        }

        return new FailOnOverflowConfig(configByColumnName, Boolean.valueOf(defaultValue));
    }
}

