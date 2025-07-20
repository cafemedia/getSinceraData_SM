from snowflake.snowpark import Session
import boto3

def func_create_snowpark_session(USER_NAME):
    """
    Function to create Snowpark session
    """
    ssm = boto3.Session(profile_name='default').client('ssm')
    SNOWFLAKE_USER = ssm.get_parameter(Name="prod.snowflake.di.snowpark.user", WithDecryption=True)['Parameter']['Value']
    SNOWFLAKE_PASSWORD = ssm.get_parameter(Name="prod.snowflake.di.snowpark.user.password", WithDecryption=True)['Parameter']['Value']
    SNOWFLAKE_ACCOUNT = ssm.get_parameter(Name='prod.snowflake.account', WithDecryption=True)['Parameter']['Value']
    connection_parameters = {"account": SNOWFLAKE_ACCOUNT, "user": SNOWFLAKE_USER, "password": SNOWFLAKE_PASSWORD}
    session = Session.builder.configs(connection_parameters).create()

    SNOWFLAKE_SCHEMA = "DI_AGGREGATIONS"
    session.sql("use schema {}".format(SNOWFLAKE_SCHEMA)).collect()
    
    my_selections = session.sql("select current_role(), current_warehouse(), current_database(), current_schema(), CONVERT_TIMEZONE('UTC', 'America/New_York', current_timestamp)").collect()
    my_lambda = lambda my_selections: "Not Specified" if my_selections[0][3] is None else my_selections[0][3]
    # print("User: " + USER_NAME)
    # print("Role: " + my_selections[0][0])
    # print("Warehouse: " + my_selections[0][1])
    # print("Database: " + my_selections[0][2])
    # print("Schema: " + my_lambda(my_selections))
    # print("Current DateTime: " + str(my_selections[0][4]))
    df_list = [[session._session_id, USER_NAME, my_selections[0][0], my_selections[0][1], my_selections[0][2], my_lambda(my_selections), my_selections[0][4]]]
    df_session = session.create_dataframe(df_list, schema=["SESSION_ID", "USER_NAME", "ROLE", "WAREHOUSE", "DATABASE", "SCHEMA", "LOGIN_DATETIME"])
    df_session.write.mode("append").save_as_table("ANALYTICS.DI_AGGREGATIONS.DI_SESSION_AUDIT")
    return session