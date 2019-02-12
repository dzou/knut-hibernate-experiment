# Spanner-Hibernate Integration Testing

This directory contains a script which allows you to run the Spanner-Hibernate dialect against
the integration tests provided in the [Hibernate ORM repository](https://github.com/hibernate/hibernate-orm).

The script will take your local working copy of `google-cloud-spanner-hibernate` and install to
local maven in order to facilitate testing, so it will allow you to test local changes.

## Instructions

1. Install Python3 on your machine.

    Verify you have it installed by running:
    
    ```SHELL
    $ python3 --version
   
    Python 3.5.4rc1 
    ```
    
2. Change directories: `cd testing/`

3. Run `python3 run-tests.py <INSERT APPROPRIATE FLAGS>`.

   Note: The script allows you to modify run with the following flags:
   
   - `--jdbc_url`: (**Required**) Set the JDBC URL to connect to the Spanner instance.
     An example JDBC URL is provided in the script.
   
     JDBC URL will be in the form: 
     
     - Simba: `jdbc:cloudspanner://;Project=gcp_project_id;Instance=spanner_instance_name;Database=my_db`
     
     - Knut: `jdbc:cloudspanner:/projects/my-project/instances/my-instance/databases/my-database`
     
   - `--dialect_class`: (Optional) Set the Hibernate dialect class to use.
   
   - `--driver_class`: (Optional) Set the JDBC driver class to use.
   
   - `--test_filter`: (Optional) Set the test filter to use.
  
   A reasonable set of defaults are provided for you; please take a look at `run-tests.py` to see the default flag values.

Notes:

JDBC Driver fully-qualified classname:

- Simba: `com.simba.cloudspanner.core.jdbc42.CloudSpanner42Driver`
- Knut: `com.google.cloud.spanner.jdbc.JdbcDriver`

Hibernate Dialect fully-qualified classname:

- Ours: `com.google.cloud.spanner.hibernate.SpannerDialect`
- Knut: `knut.dialect.CloudSpannerDialect`

Knut's JDBC driver is potentially broken... Or atleast he has offered no good way of building a jar of it.
I attempted to build a jar of it from source and obtained this error; looks like a diamond dependency issue:

```
org.hibernate.testing.junit4.CallbackException: org.hibernate.testing.junit4.BaseCoreFunctionalTestCase#buildSessionFactory
    at org.hibernate.testing.junit4.TestClassMetadata.performCallbackInvocation(TestClassMetadata.java:208)
    at org.hibernate.testing.junit4.TestClassMetadata.invokeCallback(TestClassMetadata.java:192)
...
Caused by: java.lang.NoSuchMethodError: io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.intercept(Ljava/util/List;)Lio/grpc/internal/AbstractManagedChannelImplBuilder;
    at com.google.cloud.spanner.SpannerOptions$NettyRpcChannelFactory.newChannel(SpannerOptions.java:300)
    at com.google.cloud.spanner.SpannerOptions.createChannel(SpannerOptions.java:272)
    at com.google.cloud.spanner.SpannerOptions.createChannels(SpannerOptions.java:259)
```
