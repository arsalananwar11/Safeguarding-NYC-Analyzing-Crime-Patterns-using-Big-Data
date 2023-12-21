import sys
sys.path.append("..")

from snowflake.snowpark import Session
from snowflake.snowpark.functions import when, col, count, upper
from datetime import date
from helpers import SnowflakeHelper
import json
import os

if __name__ == '__main__':
    snowflake_helper = SnowflakeHelper()
    snowflake_config = './../helpers/snowflake_config.json'
    session = snowflake_helper.create_snowpark_session(snowflake_config)
    destination_schema_name = 'SAFEGUARDING_NYC_SCHEMA_GOLD'
    uof_x_sqf_on_date = 'UOF_SQF_ON_DATE'
    uof_x_sqf_on_boro = 'UOF_SQF_ON_BORO'
    uof_x_sqf_on_precinct = 'UOF_SQF_ON_PRECINCT'

    use_of_force_combined = session.table('SAFEGUARDING_NYC_SCHEMA_GOLD.USE_OF_FORCE_COMBINED')
    sqf = session.table('SAFEGUARDING_NYC_SCHEMA_SILVER.SQF')

    agg_use_of_force = use_of_force_combined.groupBy("INCIDENT PCT").agg(count("*").alias("incident_count"))
    agg_sqf = sqf.groupBy("STOP_LOCATION_PRECINCT").agg(count("*").alias("stop_count"))

    use_of_force_and_sqf_agg = agg_use_of_force.join(
        agg_sqf, 
        agg_use_of_force["INCIDENT PCT"] == agg_sqf["STOP_LOCATION_PRECINCT"], 
        how="inner"
    ).collect()

    use_of_force_and_sqf_on_precinct = use_of_force_combined.join(
        sqf, 
        use_of_force_combined['INCIDENT PCT'] == sqf['STOP_LOCATION_PRECINCT'], 
        how="inner"
    ).collect()
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, uof_x_sqf_on_precinct, use_of_force_and_sqf_on_precinct, mode="overwrite")

    use_of_force_and_sqf_on_borough = use_of_force_combined.join(
        sqf, 
        use_of_force_combined['PATROL BOROUGH'] == sqf['STOP_LOCATION_PATROL_BORO_NAME'], 
        how="inner"
    ).collect()
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, uof_x_sqf_on_boro, use_of_force_and_sqf_on_borough, mode="overwrite")

    use_of_force_and_sqf_on_date = use_of_force_combined.join(
        sqf, 
        use_of_force_combined['OCCURRENCE DATE'] == sqf['STOP_FRISK_DATE'], 
        how="inner"
    ).collect()
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, uof_x_sqf_on_date, use_of_force_and_sqf_on_date, mode="overwrite")