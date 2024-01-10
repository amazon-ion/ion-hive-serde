/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

plugins {
    id("ion-hive-serde.dependencies")
    id("ion-hive-serde.conventions")
    id("ion-hive-serde.publishing")
}

configurations {
    implementation { extendsFrom(bundled.get(), hive3Runtime.get()) }
    testImplementation { extendsFrom(testFramework.get()) }
}

dependencies {
    bundled(project(":hive-common"))
    testFramework(testFixtures(project(":hive-common")))
}
