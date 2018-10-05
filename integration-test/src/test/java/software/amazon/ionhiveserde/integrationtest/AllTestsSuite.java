/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.integrationtest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import software.amazon.ionhiveserde.integrationtest.tests.NullMappingTest;
import software.amazon.ionhiveserde.integrationtest.tests.TypeMappingTest;

/**
 * Test for all integration tests.
 */
@RunWith(Suite.class)
@SuiteClasses({
    NullMappingTest.class,
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
