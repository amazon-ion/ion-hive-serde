// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest;

import static com.amazon.ionhiveserde.integrationtest.docker.DockerUtilKt.waitForHiveServer;

import com.amazon.ionhiveserde.integrationtest.docker.HDFS;
import com.amazon.ionhiveserde.integrationtest.tests.FailOnOverflowTest;
import com.amazon.ionhiveserde.integrationtest.tests.IgnoreMalformedTest;
import com.amazon.ionhiveserde.integrationtest.tests.NullMappingTest;
import com.amazon.ionhiveserde.integrationtest.tests.PathExtractorTest;
import com.amazon.ionhiveserde.integrationtest.tests.SerializeAsTest;
import com.amazon.ionhiveserde.integrationtest.tests.SharedSymbolTableTest;
import com.amazon.ionhiveserde.integrationtest.tests.TimestampSerializationOffsetTest;
import com.amazon.ionhiveserde.integrationtest.tests.TypeMappingTest;

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
        SerializeAsTest.Companion.setup();
        SharedSymbolTableTest.Companion.setup();
        PathExtractorTest.Companion.setup();
        IgnoreMalformedTest.Companion.setup();

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
        SerializeAsTest.Companion.tearDown();
        SharedSymbolTableTest.Companion.tearDown();
        PathExtractorTest.Companion.tearDown();
        IgnoreMalformedTest.Companion.tearDown();

        HDFS.rm("/data");
        HDFS.rm("/docker-tmp");

        Base.hive().dropAllTables();
        Base.hive().close();
    }
}

