import os
import subprocess
import argparse

import templates

'''
Script for running Hibernate Integration tests against the
Spanner-Hibernate dialect.
'''

HIBERNATE_TESTS_REPO = 'https://github.com/hibernate/hibernate-orm.git'

KNUT_JAR = 'knut-jdbc-shaded.jar'
SIMBA_JAR = 'CloudSpannerJDBC42.jar'

KNUT_DRIVER_CLASS = 'com.google.cloud.spanner.jdbc.JdbcDriver'
SIMBA_DRIVER_CLASS = 'com.simba.cloudspanner.core.jdbc42.CloudSpanner42Driver'

KNUT_DIALECT_CLASS = 'knut.dialect.CloudSpannerDialect'
OUR_DIALECT_CLASS = 'com.google.cloud.spanner.hibernate.SpannerDialect'


# Parse args
parser = argparse.ArgumentParser('Run Hibernate-orm tests specifying Hibernate Dialect and/or JDBC driver')
parser.add_argument('--dialect_class', type=str, choices=['knut', 'ours'], default='knut', help='Hibernate Dialect class for Spanner.')
parser.add_argument('--driver_class', type=str, choices=['knut', 'simba'], default='knut', help='Spanner JDBC driver class.')

# jdbc:cloudspanner:/projects/my-kubernetes-codelab-217414/instances/spring-demo/databases/experiments
# jdbc:cloudspanner://;Project=my-kubernetes-codelab-217414;Instance=spring-demo;Database=experiments
parser.add_argument('--jdbc_url', type=str, default='jdbc:cloudspanner://;Project=my-kubernetes-codelab-217414;Instance=spring-demo;Database=experiments', help='Spanner JDBC URL.')

parser.add_argument('--test_filter', type=str, default='SQLTest', help='Filters to apply on the tests to run.')
args = parser.parse_args()

jdbcJar = KNUT_JAR if args.driver_class == 'knut' else SIMBA_JAR
jdbcDriverClass = KNUT_DRIVER_CLASS if args.driver_class == 'knut' else SIMBA_DRIVER_CLASS
dialectClass = KNUT_DIALECT_CLASS if args.dialect_class == 'knut' else OUR_DIALECT_CLASS

print('Running the hibernate-orm integration tests with: ')
print('JDBC Driver = ' + jdbcDriverClass)
print('Hibernate Dialect = ' + dialectClass)


# Install
subprocess.run('mvn install -DskipTests -f ../pom.xml', shell=True)

# Clone the Hibernate Tests repository if not present.
subdirectories = [folder for folder in os.listdir('.') if os.path.isdir(folder)]
if 'hibernate-orm' in subdirectories:
  print('The hibernate-orm directory already exists; will omit cloning step.')
  subprocess.run('git -C hibernate-orm pull', shell=True)
else:
  subprocess.run(['git', 'clone', HIBERNATE_TESTS_REPO])

# Copy the JDBC driver to directory if it does not already exist.
if not os.path.isdir('hibernate-orm/libs') or \
    'CloudSpannerJDBC42.jar' not in os.listdir('hibernate-orm/libs'):
  print('Downloading Spanner JDBC driver')
  subprocess.run('gsutil cp gs://spanner-jdbc-bucket/CloudSpannerJDBC42.jar hibernate-orm/libs/CloudSpannerJDBC42.jar', shell=True)

# Patch the hibernate-orm repo with custom Gradle files for testing
subprocess.run('cp documentation.gradle hibernate-orm/documentation/documentation.gradle', shell=True)
subprocess.run('cp ../lib/knut-jdbc-shaded.jar hibernate-orm/libs/knut-jdbc-shaded.jar', shell=True)

with open('hibernate-orm/gradle/databases.gradle', 'w') as f:
  f.write(templates.DATABASE_GRADLE_TEMPLATE.format(dialect_class=dialectClass, driver_class=jdbcDriverClass, jdbc_url=args.jdbc_url))

with open('hibernate-orm/documentation/documentation.gradle', 'w') as f:
  f.write(templates.DOCUMENTATION_GRADLE_TEMPLATE.format(jdbc_jar=jdbcJar))

# Run some tests.
# Modify this to filter down to a different test with --tests TEST_NAME
subprocess.run('hibernate-orm/gradlew clean test -p hibernate-orm/documentation --tests {}'.format(args.test_filter), shell=True)
subprocess.run('google-chrome hibernate-orm/documentation/target/reports/tests/test/index.html', shell=True)
