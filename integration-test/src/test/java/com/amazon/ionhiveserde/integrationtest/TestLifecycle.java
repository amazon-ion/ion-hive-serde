// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest;

/**
 * Tests that need their lifecycle managed by {@link Lifecycle}. Objects that implement this interface must be added to
 * {@link Lifecycle#beforeClass()} and {@link Lifecycle#afterClass()}
 */
public interface TestLifecycle {

    /**
     * Setup to run before any test.
     */
    void setup();

    /**
     * Tears down anything done in {@link #setup()}.
     */
    void tearDown();
}
