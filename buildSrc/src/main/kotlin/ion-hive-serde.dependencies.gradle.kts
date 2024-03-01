// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import org.jetbrains.kotlin.gradle.plugin.extraProperties
import java.net.URI

/*
 * This manages all of the dependencies by creating configurations for different groups of dependencies that belong
 * together. These groups can be added to the standard configurations (e.g. `testImplementation`) by using the
 * `extendFrom(...)` method in the `configurations` block.
 */

// Dependencies that get bundled into the fat Jar
val bundled = configurations.create("bundled")
// Dependencies provided by Hive
val hive2Runtime = configurations.create("hive2Runtime")
val hive3Runtime = configurations.create("hive3Runtime")
// Test dependencies
val testFramework = configurations.create("testFramework")

object Versions {
    val hive2_version = "[2.3.0,2.4.0)"
    val hive3_version = "[3.1.2,3.1.17)"
    val kotlin_version = "1.9.22"
    val ionjava_version = "[1.4.0,2.0.0)"
    val pathextraction_version = "[1.2.0,2.0.0)"
    val hadoop_version = "[2.7.0,2.8.0)"
}

dependencies {
    with(Versions) {
        bundled("com.amazon.ion:ion-java:${ionjava_version}")
        bundled("com.amazon.ion:ion-java-path-extraction:${pathextraction_version}") {
            // ion-java-path-extraction uses Kotlin only for testing
            exclude(group = "org.jetbrains.kotlin")
        }

        // Provided by the Hive installation
        hive2Runtime("org.apache.hive:hive-serde:${hive2_version}")
        hive2Runtime("org.apache.hive:hive-exec:${hive2_version}") { isTransitive = false }
        hive2Runtime("org.apache.hadoop:hadoop-common:${hadoop_version}")
        hive2Runtime("org.apache.hadoop:hadoop-main:${hadoop_version}")
        hive2Runtime("org.apache.hadoop:hadoop-mapreduce-client-core:${hadoop_version}")

        // Provided by the Hive installation
        hive3Runtime("org.apache.hive:hive-serde:${hive3_version}")
        hive3Runtime("org.apache.hive:hive-exec:${hive3_version}") { isTransitive = false }
        hive3Runtime("org.apache.hadoop:hadoop-common:${hadoop_version}")
        hive3Runtime("org.apache.hadoop:hadoop-main:${hadoop_version}")
        hive3Runtime("org.apache.hadoop:hadoop-mapreduce-client-core:${hadoop_version}")

        testFramework("pl.pragmatists:JUnitParams:[1.1.0,1.2.0)")
        testFramework("org.junit.jupiter:junit-jupiter:5.7.1")
        testFramework("org.junit.vintage:junit-vintage-engine")
        testFramework("org.jetbrains.kotlin:kotlin-test-junit:$kotlin_version")
    }
}
