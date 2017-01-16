

import com.beust.kobalt.*
import com.beust.kobalt.plugin.packaging.*
import com.beust.kobalt.plugin.application.*

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

    application {
    	mainClass = "com.beust.stormlin.MainKt"
    }

    assemble {
        mavenJars {}
    }

}

