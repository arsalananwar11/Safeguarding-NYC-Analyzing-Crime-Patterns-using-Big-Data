from snowflake.snowpark import Session
import json
import os

class SnowflakeHelper():
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
    connection_parameters = self.load_config(config_filepath)
    session = Session.builder.configs(connection_parameters).create()
    print('[SUCCESS] Snowspark Session created successfully!')
    return session

if __name__ == '__main__':
    snowflake_helper = SnowflakeHelper()
    snowflake_helper.create_snowpark_session()