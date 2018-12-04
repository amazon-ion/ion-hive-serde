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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ionpathextraction.PathExtractor;
import software.amazon.ionpathextraction.PathExtractorBuilder;

/**
 * Encapsulates the path_extractor configuration.
 */
class PathExtractionConfig {

    private static final String PATH_EXTRACTOR_KEY_SUFFIX = "path_extractor";
    private static final String CASE_SENSITIVITY_KEY = "case_sensitive";

    private final Map<String, String> searchPathByColumnName;
    private final Boolean caseSensitivity;

    PathExtractionConfig(final Properties properties, final List<String> columnNames) {
        searchPathByColumnName = new HashMap<>();
        for (final String columnName : columnNames) {
            final String searchPathExpression = properties.getProperty(
                columnName + "." + PATH_EXTRACTOR_KEY_SUFFIX,
                "(" + columnName + ")");

            searchPathByColumnName.put(columnName, searchPathExpression);
        }

        caseSensitivity = Boolean.getBoolean(properties.getProperty(CASE_SENSITIVITY_KEY, "false"));
    }

    PathExtractor buildPathExtractor(final IonStruct struct, final IonSystem domFactory) {
        final PathExtractorBuilder builder = PathExtractorBuilder.standard()
            .withMatchRelativePaths(false)
            .withMatchCaseInsensitive(caseSensitivity);

        for (final Entry<String, String> entry : searchPathByColumnName.entrySet()) {
            final String columnName = entry.getKey();
            final String searchPathExpression = entry.getValue();

            builder.withSearchPath(searchPathExpression, ionReader -> {
                final IonValue ionValue = domFactory.newValue(ionReader);

                if (ionValue.isNullValue()) {
                    struct.put(columnName, null); // Hive can't handle IonNull
                } else {
                    struct.put(columnName, ionValue);
                }

                return 0;
            });
        }

        return builder.build();
    }
}
