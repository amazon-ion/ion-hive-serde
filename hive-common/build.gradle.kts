plugins {
    id("ion-hive-serde.dependencies")
    id("ion-hive-serde.conventions")
    id("java-test-fixtures")
}

// Dependencies used by the test code that are not shared with anything else
val testImplementationHive2 by configurations.creating {
    extendsFrom(configurations.testImplementation.get(), configurations.hive2Runtime.get())
}
val testImplementationHive3 by configurations.creating {
    extendsFrom(configurations.testImplementation.get(), configurations.hive3Runtime.get())
}

configurations {
    implementation { extendsFrom(bundled.get()) }
    // Have to pick one of the hive versions in order to get everything to compile. However, we will also compile
    // using the other Hive version, and we will run the tests using both runtimes.
    compileOnly { extendsFrom(hive3Runtime.get()) }
    testCompileOnly { extendsFrom(hive3Runtime.get()) }

    // testImplementation also extendsFrom testFixturesImplementation
    testFixturesImplementation { extendsFrom(configurations.testFramework.get()) }
}

dependencies {
    testImplementationHive2(testFixtures(project))
    testImplementationHive3(testFixtures(project))
}

tasks {
    test {
        classpath = testImplementationHive3 + project.sourceSets.test.get().output
    }

    val compileJavaHive2 = create<JavaCompile>("compileJavaHive2") {
        classpath = configurations.hive2Runtime.get() + configurations.bundled.get()
    }

    val testHive2 = create<Test>("testHive2") {
        dependsOn(compileJavaHive2)
        classpath = testImplementationHive2 + project.sourceSets.test.get().output
        useJUnitPlatform()
    }

    check { dependsOn(testHive2) }
}
