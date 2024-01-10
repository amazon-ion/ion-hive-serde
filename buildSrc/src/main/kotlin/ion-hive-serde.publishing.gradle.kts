// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.bundling.Jar
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.get
import org.gradle.kotlin.dsl.kotlin
import org.jetbrains.kotlin.gradle.plugin.extraProperties
import java.net.URI

repositories {
    mavenCentral()
}

plugins {
    kotlin("jvm")
    id("signing")
    id("maven-publish")
    id("com.github.johnrengelman.shadow")
    id("ion-hive-serde.dependencies")
}

tasks {
    withType<Jar>().all {
        archiveBaseName.set("ion-${project.name}-serde")
    }

    shadowJar {
        configurations = listOf(project.configurations.getByName("bundled"))
    }

    val testShadowJar = create<Test>("testShadowJar") {
        group = "verification"
        dependsOn(shadowJar)
        classpath = shadowJar.get().outputs.files +
                project.configurations.getByName("${project.name}Runtime") +
                project.configurations.getByName("testFramework") +
                project.sourceSets.test.get().output
    }

    check {
        dependsOn(testShadowJar)
    }

    assemble {
        dependsOn(shadowJar)
    }
}

val sourcesJar = tasks.create<Jar>("sourcesJar") {
    archiveClassifier.set("sources")
    from(sourceSets["main"].java.srcDirs)
}

val javadocJar = tasks.create<Jar>("javadocJar") {
    archiveClassifier.set("javadoc")
    from(tasks.javadoc.get())
}

publishing {
    publications {
        publications.create<MavenPublication>("mavenJava") {
            artifactId = tasks.jar.get().archiveBaseName.get()
            artifact(tasks.jar)
            artifact(sourcesJar)
            artifact(javadocJar)

            pom {
                name.set("Ion Hive Serde")
                description.set("An Apache Hive SerDe (short for serializer/deserializer) for the Ion file format.")
                url.set("https://github.com/amazon-ion/ion-hive-serde/")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        name.set("Amazon Ion Team")
                        email.set("ion-team@amazon.com")
                        organization.set("Amazon Ion")
                        organizationUrl.set("https://github.com/amazon-ion")
                    }
                }
                scm {
                    connection.set("scm:git:git@github.com:amazon-ion/ion-hive-serde.git")
                    developerConnection.set("scm:git:git@github.com:amazon-ion/ion-hive-serde.git")
                    url.set("git@github.com:amazon-ion/ion-hive-serde.git")
                }
            }
        }
        repositories.mavenCentral {
            credentials {
                username = properties["ossrhUsername"].toString()
                password = properties["ossrhPassword"].toString()
            }
            url = URI.create("https://aws.oss.sonatype.org/service/local/staging/deploy/maven2")
        }
    }
}

signing {
    sign(publishing.publications["mavenJava"])
}
