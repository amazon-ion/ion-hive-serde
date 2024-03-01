// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration;

import com.amazon.ionhiveserde.configuration.source.RawConfiguration;

/**
 * Encapsulates Ion related Hadoop job properties.
 */
public class HadoopProperties extends BaseProperties {

    /**
     * Constructor.
     *
     * @param configuration raw configuration.
     */
    public HadoopProperties(final RawConfiguration configuration) {
        super(configuration);
    }
}
