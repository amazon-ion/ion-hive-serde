// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration;

/**
 * Possible strategies for the serialize_null property.
 */
public enum SerializeNullStrategy {
    /**
     * Omit nulls.
     */
    OMIT,

    /**
     * Serialize strongly typed nulls.
     */
    TYPED,

    /**
     * Serialize untyped nulls.
     */
    UNTYPED;
}
