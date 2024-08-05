## Welcome to your First Snowflake Native App!

In this Snowflake Native App, you will be able to explore some basic concepts such as application role, versioned schemas and creating procedures and functions within a setup script.

For more information about a Snowflake Native App, please read the [official Snowflake documentation](https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about) which goes in depth about many additional functionalities of this framework.

## Using the application after installation
To interact with the application after it has successfully installed in your account, switch to the application owner role first.

### Calling a stored procedure

```
CALL <your_application_name>.<schema_name>.<stored_procedure_name_with_args>;
```

### Calling a function

```
SELECT <your_application_name>.<schema_name>.<udf_with_args>;
```
