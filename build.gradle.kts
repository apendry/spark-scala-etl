plugins {
    id("java")
    id("scala")
}

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
    }

    plugins.apply("java")
    plugins.apply("scala")
    dependencies {
        implementation("org.scala-lang:scala-library:2.13.12")

        testImplementation("org.scalatest:scalatest_2.13:3.2.19")
    }
}