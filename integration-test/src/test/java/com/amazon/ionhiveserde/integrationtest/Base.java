// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest;

import com.amazon.ionhiveserde.integrationtest.docker.Hive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Base class for all integration tests. Handles the connection lifecycle to reuse the same connection as much as
 * possible.
 */
public abstract class Base {

    static TestMode testMode = TestMode.CLASS;
    private static Hive hive = null;

    protected static Hive hive() {
        if (hive == null || hive.isClosed()) {
            hive = new Hive();
        }

        return hive;
    }

    /**
     * Only runs when test mode is set to {@link TestMode#CLASS}.
     *
     * @see Lifecycle#beforeClass
     */
    @BeforeClass
    public static void beforeClass() {
        if (testMode == TestMode.CLASS) {
            Lifecycle.beforeClass();
        }
    }

    /**
     * Only runs when test mode is set to {@link TestMode#CLASS}.
     *
     * @see Lifecycle#afterClass
     */
    @AfterClass
    public static void afterClass() {
        if (testMode == TestMode.CLASS) {
            Lifecycle.afterClass();
        }
    }

    /**
     * Drops all hive tables to ensure a clean metastore. This will not clean up HDFS, it only removes the Hive tables
     * which are views on the HDFS test data.
     */
    @Before
    public void before() {
        hive().dropAllTables();
    }
}
