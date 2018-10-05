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

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;

/**
 * Encapsulates all SerDe properties.
 */
class SerDeProperties {

    private static final String ENCODING_KEY = "encoding";
    private final IonEncoding encoding;

    /**
     * Constructor.
     *
     * @param properties {@link Properties} passed to {@link IonHiveSerDe#initialize(Configuration, Properties)}
     */
    SerDeProperties(final Properties properties) {
        encoding = IonEncoding.valueOf(properties.getProperty(ENCODING_KEY, IonEncoding.BINARY.name()));
    }

    /**
     * {@link IonEncoding} to be used when serializing, i.e. if it serializes to text or binary Ion.
     *
     * @return IonEncoding to be used.
     */
    IonEncoding getEncoding() {
        return encoding;
    }
}
