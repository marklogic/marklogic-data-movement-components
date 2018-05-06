This directory contains Java classes that can be used to export data out of MarkLogic and into a Tableau Data Extract file.

These classes are not included in the src/main/java directory because they depend on Tableau JARs that are not available via any public repository. 
Thus, in order to use these classes, you will need to obtain the Tableau JARs and compile the code yourself. Instructions for doing so are below.

## Obtain the Tableau JARs

The Tableau JARs can be downloaded from this link - [Tableau SDK](https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm)

The Tableau JARs can be stored in any location that Gradle can access. One simple option is to make a "lib" directory 
and copy the files there.

The "dependencies" block in the build.gradle file must then be updated so that the Tableau JARs are used for compilation. 
As an example, if the JARs are were copied to a "lib" directory, the following lines would be added to the "dependencies" block:

    compile files("lib/tableaucommon.jar") 
    compile files("lib/tableauextract.jar")
    compile files("lib/tableauserver.jar")
    compile files("lib/jna.jar")

## Compile the Tableau-dependent source files

Note - in order to compile these source files, this project must depend on marklogic-client-api version 4.0.4 or higher. 
Please check this project's build.gradle file to verify this. Also, Tableau Extract feature requires MarkLogic server version 9.0.5 or higher.

The source file that depend on the Tableau JARs are stored under src/tableau/main/java. These files must be copied to 
src/main/java so that they are included in the JAR produced by this project. 

You can verify that the source files are compiled correctly and that you correctly updated the build.gradle file by running
the following Gradle task:

    gradle compileJava

## Publish a copy of marklogic-data-movement-components

Because the Tableau source files are not in src/main/java by default, they are not included in the JAR for this project 
that is published to remote Maven repositories like jcenter. Now that you've added the source files to src/main/java and
verified that you can compile them, you'll need to publish a copy of marklogic-data-movement-components that you can then
include e.g. in your own Maven repository. To do so, run the following Gradle task:

    gradle -Pversion=1.0.TABLEAU publishToMavenLocal

The "version" property can have any value; it is helpful to have some indicator on it that this is not a normal 
published version of marklogic-data-movement-components.

By running this task, Gradle will publish the JAR, sources JAR, and POM file to the following location:

    ~/.m2/repository/com/marklogic/marklogic-data-movement-components/1.0.TABLEAU
    
## Using the published copy of marklogic-data-movement-components

At this point, what you do next depends on how to want to use your published copy of marklogic-data-movement-components:

1. To use the published JAR as a dependency in another project, add "mavenLocal()" as a repository to the "buildscript" 
block in that project's build.gradle file. 
1. You may have an internal Maven repository such as Nexus where you manage dependencies; if so, you can upload the artifacts
you published locally to that repository, thus allowing you to reference these as normal dependencies without depending on "mavenLocal()"

In either case, or in some other approach, you'll need to ensure that the client of your published copy of 
marklogic-data-movement-components also has access to the Tableau JARs. 
