import sys
sys.path.append("..")

from snowflake.snowpark import Session
from snowflake.snowpark.functions import when, col, count
from datetime import date
from helpers import SnowflakeHelper
import json
import os

if __name__ == '__main__':
    source_schema_name = "SAFEGUARDING_NYC_SCHEMA_SILVER"
    snowflake_helper = SnowflakeHelper(source_schema_name)
    snowflake_config = './../helpers/snowflake_config.json'
    session = snowflake_helper.create_snowpark_session(snowflake_config)
    destination_schema_name = 'SAFEGUARDING_NYC_SCHEMA_GOLD'
    shooting_incidents_x_sqf_on_date = 'SI_SQF_ON_DATE'
    shooting_incidents_x_sqf_on_boro = 'SI_SQF_ON_BORO'
    shooting_incidents_x_sqf_on_precinct = 'SI_SQF_ON_PRECINCT'

    shooting_incidents = session.table('SHOOTING_INCIDENTS')
    sqf_data = session.table('SQF')

    shooting_sqf_on_date = shooting_incidents.join(
        sqf_data, 
        shooting_incidents["OCCUR_DATE"] == sqf_data["STOP_FRISK_DATE"], 
        how="inner"
    ).collect()
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, shooting_incidents_x_sqf_on_date, shooting_sqf_on_date, mode="overwrite")

    shooting_sqf_on_boro = shooting_incidents.join(
        sqf_data, 
        shooting_incidents["BORO"] == sqf_data["STOP_LOCATION_BORO_NAME"], 
        how="inner"
    ).collect()
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, shooting_incidents_x_sqf_on_boro, shooting_sqf_on_boro, mode="overwrite")

    shooting_sqf_on_prec = shooting_incidents.join(
        sqf_data,
        shooting_incidents["PRECINCT"] == sqf_data["STOP_LOCATION_PRECINCT"], 
        how="inner"
    ).collect()
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, shooting_incidents_x_sqf_on_precinct, shooting_sqf_on_prec, mode="overwrite")