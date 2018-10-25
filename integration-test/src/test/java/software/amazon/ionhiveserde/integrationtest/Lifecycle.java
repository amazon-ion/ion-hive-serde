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

package software.amazon.ionhiveserde.integrationtest;

import static software.amazon.ionhiveserde.integrationtest.Base.hive;
import static software.amazon.ionhiveserde.integrationtest.docker.DockerUtilKt.waitForHiveServer;

import software.amazon.ionhiveserde.integrationtest.docker.HDFS;
import software.amazon.ionhiveserde.integrationtest.tests.FailOnOverflowTest;
import software.amazon.ionhiveserde.integrationtest.tests.NullMappingTest;
import software.amazon.ionhiveserde.integrationtest.tests.TimestampSerializationOffsetTest;
import software.amazon.ionhiveserde.integrationtest.tests.TypeMappingTest;

/**
 * Encapsulates integration testing lifecycle to reuse between class and suite runs.
 */
class Lifecycle {
    /**
     * Waits for hive server to be receiving connections and sets up all test data in HDFS.
     */
    static void beforeClass() {
        waitForHiveServer();

        NullMappingTest.Companion.setup();
        TypeMappingTest.Companion.setup();
        TimestampSerializationOffsetTest.Companion.setup();
        FailOnOverflowTest.Companion.setup();

        // puts all test data into hdfs at once
        HDFS.put("input/");
    }

    /**
     * Fully cleans up test context.
     * <ol>
     *     <li>Destroy test data.</li>
     *     <li>Cleans created HDFS files.</li>
     *     <li>drops all hive tables.</li>
     *     <li>closes hive connection.</li>
     * </ol>
     */
    static void afterClass() {
        NullMappingTest.Companion.tearDown();
        TypeMappingTest.Companion.tearDown();
        TimestampSerializationOffsetTest.Companion.tearDown();
        FailOnOverflowTest.Companion.tearDown();

        HDFS.rm("/data");
        HDFS.rm("/docker-tmp");

        hive().dropAllTables();
        hive().close();
    }
}

