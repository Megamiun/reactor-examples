import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension

plugins {
    kotlin("jvm") version "1.4.0"    
    id("io.spring.dependency-management") version "1.0.7.RELEASE"
}

configure<DependencyManagementExtension> {
    imports { mavenBom("io.projectreactor:reactor-bom:Bismuth-RELEASE") }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("io.projectreactor:reactor-core")
}
