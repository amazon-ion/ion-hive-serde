// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

plugins {
    id("ion-hive-serde.dependencies")
    id("ion-hive-serde.conventions")
    id("ion-hive-serde.publishing")
}

configurations {
    implementation { extendsFrom(bundled.get(), hive2Runtime.get()) }
    testImplementation { extendsFrom(testFramework.get()) }
}

dependencies {
    bundled(project(":hive-common"))
    testFramework(testFixtures(project(":hive-common")))
}
