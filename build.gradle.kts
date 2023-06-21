plugins {
    id("java")
}

group = "com.datastax"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.datastax.oss:java-driver-core:4.16.0")
}
