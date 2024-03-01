// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

repositories {
    gradlePluginPortal()
    mavenCentral()
}

plugins {
    `kotlin-dsl`
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:1.9.22")
    implementation("com.github.johnrengelman:shadow:8.1.1")
}
