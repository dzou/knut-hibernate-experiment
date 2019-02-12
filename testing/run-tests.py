import os
import subprocess
import argparse

'''
Script for running Hibernate Integration tests against the
Spanner-Hibernate dialect.
'''

HIBERNATE_TESTS_REPO = 'https://github.com/hibernate/hibernate-orm.git'

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

# Parse args
parser = argparse.ArgumentParser('Run test using different jdbc driver or dialect.')
parser.add_argument('--dialect_class', type=str, default='com.google.cloud.spanner.hibernate.SpannerDialect', help='Hibernate Dialect class for Spanner.')
parser.add_argument('--driver_class', type=str, default='com.simba.cloudspanner.core.jdbc42.CloudSpanner42Driver', help='Spanner JDBC driver class.')
parser.add_argument('--jdbc_url', type=str, default='jdbc:cloudspanner://;Project=my-kubernetes-codelab-217414;Instance=spring-demo;Database=experiments', help='Spanner JDBC URL.')
parser.add_argument('--test_filter', type=str, default='SQLTest', help='Filters to apply on the tests to run.')
args = parser.parse_args()

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
  f.write(DATABASE_GRADLE_TEMPLATE.format(dialect_class=args.dialect_class, driver_class=args.driver_class, jdbc_url=args.jdbc_url))

# Run some tests.
# Modify this to filter down to a different test with --tests TEST_NAME
subprocess.run('hibernate-orm/gradlew clean test -p hibernate-orm/documentation --tests {}'.format(args.test_filter), shell=True)
subprocess.run('google-chrome hibernate-orm/documentation/target/reports/tests/test/index.html', shell=True)
