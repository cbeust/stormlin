

import com.beust.kobalt.plugin.application.application
import com.beust.kobalt.plugin.packaging.assemble
import com.beust.kobalt.project

val p = project {
    name = "stormlin"
    group = "com.beust"
    artifactId = name
    version = "0.1"

    dependencies {
        compile(
        	"org.jetbrains.kotlin:kotlin-reflect:1.0.6",
        	"mysql:mysql-connector-java:6.0.5")
    }

    dependenciesTest {
        compile("org.testng:testng:6.10",
                "org.assertj:assertj-core:3.5.2",
                "com.almworks.sqlite4java:sqlite4java:1.0.392",
                "org.xerial:sqlite-jdbc:3.16.1")
    }

    application {
    	mainClass = "com.beust.stormlin.MainKt"
    }

    assemble {
        mavenJars {}
    }

}

