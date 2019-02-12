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

2. Run `python3 run-tests.py <INSERT APPROPRIATE FLAGS>`.

   Note: The script allows you to modify run with the following flags:
   
   - `--jdbc_url`: (**Required**) Set the JDBC URL to connect to the Spanner instance.
     An example JDBC URL is provided in the script.
   
     JDBC URL will be in the form: `jdbc:cloudspanner://;Project=gcp_project_id;Instance=spanner_instance_name;Database=my_db`
     
   - `--dialect_class`: (Optional) Set the Hibernate dialect class to use.
   
   - `--driver_class`: (Optional) Set the JDBC driver class to use.
   
   - `--test_filter`: (Optional) Set the test filter to use.
  
   A reasonable set of defaults are provided for you; please take a look at `run-tests.py` to see the default flag values.
