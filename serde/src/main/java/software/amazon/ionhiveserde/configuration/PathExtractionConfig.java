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

package software.amazon.ionhiveserde.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonValue;
import software.amazon.ionhiveserde.configuration.source.RawConfiguration;
import software.amazon.ionpathextraction.PathExtractor;
import software.amazon.ionpathextraction.PathExtractorBuilder;

/**
 * Encapsulates the path_extractor configuration.
 */
class PathExtractionConfig {

    private static final String PATH_EXTRACTOR_KEY_FORMAT = "ion.%s.path_extractor";
    private static final String PATH_EXTRACTOR_DEFAULT_FORMAT = "( %s )";
    private static final String CASE_SENSITIVITY_KEY = "ion.path_extractor.case_sensitive";

    private final Map<String, String> searchPathByColumnName;
    private final Boolean caseSensitivity;

    /**
     * Constructor.
     *
     * @param configuration raw configuration.
     * @param columnNames table column names.
     */
    PathExtractionConfig(final RawConfiguration configuration, final List<String> columnNames) {
        searchPathByColumnName = new HashMap<>();
        for (final String columnName : columnNames) {
            final String searchPathExpression = configuration.getOrDefault(
                String.format(PATH_EXTRACTOR_KEY_FORMAT, columnName),
                String.format(PATH_EXTRACTOR_DEFAULT_FORMAT, columnName));

            searchPathByColumnName.put(columnName, searchPathExpression);
        }

        caseSensitivity = Boolean.getBoolean(configuration.getOrDefault(CASE_SENSITIVITY_KEY, "false"));
    }

    /**
     * Builds a path extractor from configuration that will accumulated matched paths to the struct.
     *
     * @param struct mutable struct to accumulated matched paths.
     * @param domFactory Ion system used to create DOM objects.
     *
     * @return PathExtractor configured for matching.
     */
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
