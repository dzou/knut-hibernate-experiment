import os
import subprocess

'''
Script for running Hibernate Integration tests against the
Spanner-Hibernate dialect.
'''

HIBERNATE_TESTS_REPO = 'https://github.com/hibernate/hibernate-orm.git'

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
subprocess.run('cp databases.gradle hibernate-orm/gradle/databases.gradle', shell=True)
subprocess.run('cp documentation.gradle hibernate-orm/documentation/documentation.gradle', shell=True)

# Run some tests.
# Modify this to filter down to a different test with --tests TEST_NAME
subprocess.run('hibernate-orm/gradlew clean test -p hibernate-orm/documentation --tests SQLTest', shell=True)
subprocess.run('google-chrome hibernate-orm/documentation/target/reports/tests/test/index.html', shell=True)
