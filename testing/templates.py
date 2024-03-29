DATABASE_GRADLE_TEMPLATE = """
ext {{
        db = 'spanner'
        dbBundle = [
                spanner : [
                        'db.dialect' : '{dialect_class}',
                        'jdbc.driver' : '{driver_class}',
                        'jdbc.user' : '',
                        'jdbc.pass' : '',
                        'jdbc.url' : '{jdbc_url}',
                ],
        ]
}}
"""

DOCUMENTATION_GRADLE_TEMPLATE = """
apply from: rootProject.file( 'gradle/java-module.gradle' )

apply plugin: 'hibernate-matrix-testing'

repositories {{
    mavenLocal()
}}

dependencies {{
    ext.pressgangVersion = '3.0.0'

    // Spanner-Hibernate testing deps
    compile files("$rootProject.projectDir/libs/{jdbc_jar}")
    testCompile "our-team:spanner-hibernate-comparison:1.0-SNAPSHOT"

    compile( libraries.jpa )
    compile( project( ':hibernate-core' ) )
    annotationProcessor( project( ':hibernate-jpamodelgen' ) )

    testCompile( 'org.apache.commons:commons-lang3:3.4' )

    testCompile( project(':hibernate-envers') )
    testCompile( project(':hibernate-spatial') )
    testCompile( project(path: ':hibernate-core', configuration: 'tests') )

    testCompile( project(':hibernate-testing') )

    testCompile "org.osgi:org.osgi.core:4.3.1"

    testCompile( libraries.mockito )
    testCompile( libraries.mockito_inline )

    testCompile( project( ':hibernate-jcache' ) )
    testRuntime( libraries.ehcache3 )
}}

// Not used; needed because this task is referenced in other gradle files.
task buildDocsForPublishing {{
    group 'Documentation'
    description 'Grouping task for building all documentation for publishing (release)'
}}
"""
