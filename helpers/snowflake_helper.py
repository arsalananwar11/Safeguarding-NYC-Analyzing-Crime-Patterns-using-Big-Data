from snowflake.snowpark import Session
import json
import os

class SnowflakeHelper():
  def __init__(self, schema_name = "SAFEGUARDING_NYC_SCHEMA_BRONZE"):
    self.current_schema = schema_name
    if self.current_schema == "SAFEGUARDING_NYC_SCHEMA_BRONZE":
      print(f'[INFO] No schema passed, using default schema { self.current_schema } for the session')
    else:
      print(f'[INFO] Using the schema { self.current_schema } for the session')
    
  def load_config(self, filepath):
    try:
      config = open(filepath)
      config = json.load(config)
      print('[SUCCESS] Config file loaded successfully!')
      return config
    except Exception:
      print('[ERROR] Some error occured while loading the config file. Please check the location/file path of the config file')
      return None

  def create_snowpark_session(self, config_filepath = './snowflake_config.json'):
    try:
      connection_parameters = self.load_config(config_filepath)
      if connection_parameters != None:
        session = Session.builder.configs(connection_parameters).create()
        session.use_schema(self.current_schema)
        print(f'[SUCCESS] Snowspark Session created successfully on schema { self.current_schema }!')
        return session
      else:
        return None
    except:
      print('[ERROR] Some error occured while creating the snowspark session. Please check the config file settings')
      return None
    
  def save_data_in_snowflake(self, session, schema_name, table_name, data, mode = "overwrite"):
    try:
      schema_table_ptr = f'{ schema_name }.{ table_name }'
      session.use_schema(schema_name)
      data.write.saveAsTable(table_name , mode = mode)
      session.use_schema(self.current_schema)
      print(f'[SUCCESS] Data saved successfully in { schema_table_ptr } table in Snowflake!')
    except:
      print(f'[ERROR] Some error occured while saving the data in { schema_table_ptr } table snowflake. Please check the schema and the data')
      return None

if __name__ == '__main__':
    snowflake_helper = SnowflakeHelper()
    snowflake_helper.create_snowpark_session()