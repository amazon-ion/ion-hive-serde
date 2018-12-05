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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import software.amazon.ion.IonType;
import software.amazon.ionhiveserde.configuration.source.RawConfiguration;

/**
 * Encapsulates the serialize_as configuration.
 */
class SerializeAsConfig {

    private static final String SERIALIZE_AS_KEY_FORMAT = "ion.column[%d].serialize_as";
    private static final Pattern NORMALIZE_PATTERN = Pattern.compile("([a-zA-Z]+).*");

    // first type is the default
    private static final HashMap<String, List<IonType>> VALID_MAPPINGS;

    static {
        VALID_MAPPINGS = new HashMap<>();
        // Fixed
        VALID_MAPPINGS.put(serdeConstants.BOOLEAN_TYPE_NAME, Collections.singletonList(IonType.BOOL));
        VALID_MAPPINGS.put(serdeConstants.TINYINT_TYPE_NAME, Collections.singletonList(IonType.INT));
        VALID_MAPPINGS.put(serdeConstants.SMALLINT_TYPE_NAME, Collections.singletonList(IonType.INT));
        VALID_MAPPINGS.put(serdeConstants.INT_TYPE_NAME, Collections.singletonList(IonType.INT));
        VALID_MAPPINGS.put(serdeConstants.BIGINT_TYPE_NAME, Collections.singletonList(IonType.INT));
        VALID_MAPPINGS.put(serdeConstants.FLOAT_TYPE_NAME, Collections.singletonList(IonType.FLOAT));
        VALID_MAPPINGS.put(serdeConstants.DOUBLE_TYPE_NAME, Collections.singletonList(IonType.FLOAT));
        VALID_MAPPINGS.put(serdeConstants.DATE_TYPE_NAME, Collections.singletonList(IonType.TIMESTAMP));
        VALID_MAPPINGS.put(serdeConstants.TIMESTAMP_TYPE_NAME, Collections.singletonList(IonType.TIMESTAMP));

        // Configurable
        VALID_MAPPINGS.put(serdeConstants.STRING_TYPE_NAME, Arrays.asList(IonType.STRING, IonType.SYMBOL));
        VALID_MAPPINGS.put(serdeConstants.CHAR_TYPE_NAME, Arrays.asList(IonType.STRING, IonType.SYMBOL));
        VALID_MAPPINGS.put(serdeConstants.VARCHAR_TYPE_NAME, Arrays.asList(IonType.STRING, IonType.SYMBOL));
        VALID_MAPPINGS.put(serdeConstants.BINARY_TYPE_NAME, Arrays.asList(IonType.BLOB, IonType.CLOB));
        VALID_MAPPINGS.put(serdeConstants.DECIMAL_TYPE_NAME, Arrays.asList(IonType.DECIMAL, IonType.INT));

        VALID_MAPPINGS.put(serdeConstants.LIST_TYPE_NAME, Arrays.asList(IonType.LIST, IonType.SEXP));
        VALID_MAPPINGS.put(serdeConstants.MAP_TYPE_NAME, Collections.singletonList(IonType.STRUCT));
        VALID_MAPPINGS.put(serdeConstants.STRUCT_TYPE_NAME, Collections.singletonList(IonType.STRUCT));

        // FIXME what to do here
        // VALID_MAPPINGS.put(serdeConstants.UNION_TYPE_NAME, Collections.singletonList(IonType.INT));
    }

    private static IonType defaultFor(final String typeName) {
        List<IonType> ionTypes = VALID_MAPPINGS.get(typeName);
        if (ionTypes == null) {
            throw new IllegalArgumentException("no valid serialization mappings for " + typeName);
        }

        return ionTypes.get(0);
    }

    private static String normalize(final String typeName) {
        final Matcher matcher = NORMALIZE_PATTERN.matcher(typeName);
        if (!matcher.find()) {
            throw new IllegalStateException("could not normalize typeName: " + typeName);
        }

        return matcher.group(1);
    }

    private static void validate(final IonType ionType, final String typeName) {
        final List<IonType> ionTypes = VALID_MAPPINGS.get(typeName);

        if (ionTypes == null || !ionTypes.contains(ionType)) {
            throw new IllegalArgumentException("The hive type: "
                + typeName
                + " can not be serialized as the Ion type: "
                + ionType);
        }
    }

    private final List<IonType> ionTypeByIndex;

    /**
     * Constructor.
     *
     * @param configuration raw configuration source.
     * @param columnTypes table column types.
     */
    SerializeAsConfig(final RawConfiguration configuration,
                      final List<TypeInfo> columnTypes) {

        ionTypeByIndex = new ArrayList<>();

        for (int i = 0; i < columnTypes.size(); i++) {
            final Optional<String> ionTypeName = configuration.get(String.format(SERIALIZE_AS_KEY_FORMAT, i));
            final String typeInfoName = normalize(columnTypes.get(i).getTypeName());

            final IonType ionType = ionTypeName.map(t -> IonType.valueOf(t.toUpperCase()))
                .orElse(SerializeAsConfig.defaultFor(typeInfoName));

            validate(ionType, typeInfoName);

            ionTypeByIndex.add(i, ionType);
        }
    }

    /**
     * Returns the Ion type to be used during serialization for the column.
     *
     * @return the Ion type to be used during serialization for the column.
     */
    IonType serializationIonTypeFor(final int index) {
        return ionTypeByIndex.get(index);
    }
}
