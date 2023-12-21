import sys
sys.path.append("..")

from snowflake.snowpark import Session
from snowflake.snowpark.functions import when, col, count
from datetime import date
from helpers import SnowflakeHelper
import json
import os

if __name__ == '__main__':
    snowflake_helper = SnowflakeHelper()
    snowflake_config = './../helpers/snowflake_config.json'
    session = snowflake_helper.create_snowpark_session(snowflake_config)
    session.use_schema("SAFEGUARDING_NYC_SCHEMA_SILVER")
    destination_schema_name = 'SAFEGUARDING_NYC_SCHEMA_GOLD'
    destination_table_name = 'USE_OF_FORCE_COMBINED'

    use_of_force_incidents = session.table('use_of_force_incidents')
    use_of_force_subjects = session.table('use_of_force_subjects')

    use_of_force_combined = use_of_force_incidents.join(
        use_of_force_subjects, 
        use_of_force_incidents['TRI INCIDENT NUMBER'] == use_of_force_subjects['TRI INCIDENT NUMBER'],
        how="inner"
    ).collect()

    use_of_force_combined = use_of_force_combined.drop(['r_cgks_TRI INCIDENT NUMBER'])
    use_of_force_combined = use_of_force_combined.withColumnRenamed("l_d90k_TRI INCIDENT NUMBER", "TRI INCIDENT NUMBER")
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, destination_table_name, use_of_force_combined, mode="overwrite")