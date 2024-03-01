// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest;

import com.amazon.ionhiveserde.integrationtest.tests.FailOnOverflowTest;
import com.amazon.ionhiveserde.integrationtest.tests.IgnoreMalformedTest;
import com.amazon.ionhiveserde.integrationtest.tests.NullMappingTest;
import com.amazon.ionhiveserde.integrationtest.tests.PathExtractorTest;
import com.amazon.ionhiveserde.integrationtest.tests.SerializeAsTest;
import com.amazon.ionhiveserde.integrationtest.tests.SharedSymbolTableTest;
import com.amazon.ionhiveserde.integrationtest.tests.TimestampSerializationOffsetTest;
import com.amazon.ionhiveserde.integrationtest.tests.TypeMappingTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test for all integration tests.
 */
@RunWith(Suite.class)
@SuiteClasses({
    FailOnOverflowTest.class,
    IgnoreMalformedTest.class,
    NullMappingTest.class,
    PathExtractorTest.class,
    SerializeAsTest.class,
    SharedSymbolTableTest.class,
    TimestampSerializationOffsetTest.class,
    TypeMappingTest.class
})
public class AllTestsSuite {

    /**
     * Before all tests.
     *
     * @see Lifecycle#beforeClass
     */
    @BeforeClass
    public static void beforeClass() {
        // disables per class lifecycle to speed tests up
        Base.testMode = TestMode.SUITE;
        Lifecycle.beforeClass();
    }

    /**
     * After all tests.
     *
     * @see Lifecycle#afterClass()
     */
    @AfterClass
    public static void afterClass() {
        Lifecycle.afterClass();
    }
}
