import org.jetbrains.kotlin.gradle.plugin.extraProperties
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.net.URI

repositories {
    mavenCentral()
}

plugins {
    kotlin("jvm")
    id("checkstyle")
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

checkstyle {
    toolVersion = "8.18"
    // TODO: Fix checkstyle failures
    isIgnoreFailures = true
    maxWarnings = 0
    maxErrors = 0
}

tasks {

    withType<JavaCompile>().all {
        // The release option is not available for the Java 8 compiler, but that's okay because we're targeting 8.
        if (JavaVersion.current() != JavaVersion.VERSION_1_8) {
            options.release.set(8)
        }
    }

    withType<KotlinCompile>().all {
        kotlinOptions {
            jvmTarget = "1.8"
        }
    }

    withType<Test>().all {
        useJUnitPlatform()
        doLast {
            logger.quiet("Test report written to: file://${reports.html.entryPoint}")
        }
    }

    withType<Checkstyle>().all {
        reports {
            xml.required.set(false)
            html.required.set(true)
        }
    }
}
