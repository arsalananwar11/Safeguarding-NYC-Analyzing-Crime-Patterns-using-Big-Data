import sys
sys.path.append("..")
from snowflake.snowpark import Session, dataframe
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import when, col, to_date, count
from helpers import SnowflakeHelper
from snowflake.snowpark.functions import date_format
import json
import os

null_value_mapping = {
    "(null)": None,
    "Nan": None,
    "NONE": None,
    "nan": None,
    "U": None,
    "UNKNOWN": None
}

age_mapping = {
    "224": None, 
    "1020": None, 
    "940": None, 
    "1022": None
}

race_mapping = {
    "ASIAN / PACIFIC ISLANDER" : "ASIAN/PACIFIC ISLANDER"
}

gender_mapping = {
    "M": "MALE",
    "F": "FEMALE"
}

if __name__ == '__main__':
    snowflake_helper = SnowflakeHelper()
    snowflake_config = './../helpers/snowflake_config.json'
    session = snowflake_helper.create_snowpark_session(snowflake_config)
    session.use_schema("SAFEGUARDING_NYC_SCHEMA_BRONZE")
    destination_schema_name = 'SAFEGUARDING_NYC_SCHEMA_SILVER'
    nypd_precincts_table_name = 'NYPD_PRECINCTS_NEIGHBORHOODS'
    shooting_data_table_name = "SHOOTING_INCIDENTS"

    nypd_precincts = session.table('NYPD_PRECINCTS_NEIGHBORHOODS')
    nypd_precincts = nypd_precincts.drop(["_AIRBYTE_RAW_ID", "_AIRBYTE_EXTRACTED_AT", "_AIRBYTE_META"]).collect()
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, nypd_precincts_table_name, nypd_precincts, mode="overwrite")

    shooting_data = session.table("SHOOTING_INCIDENTS")
    columns_to_drop = ["_AIRBYTE_RAW_ID", "_AIRBYTE_EXTRACTED_AT", "_AIRBYTE_META", "X_COORD_CD", "Y_COORD_CD", "LON_LAT", "JURISDICTION_CODE", "LOC_OF_OCCUR_DESC", "LOC_CLASSFCTN_DESC"]
    shooting_data = shooting_data.drop(*columns_to_drop).collect()
    shooting_data = shooting_data.withColumn("OCCUR_DATE", when(col("OCCUR_DATE").isNotNull(), to_date(col("OCCUR_DATE"), 'MM/DD/YYYY')).otherwise(None)).collect()

    check_columns = ["VIC_RACE", "VIC_AGE_GROUP", "PERP_RACE", "STATISTICAL_MURDER_FLAG", "VIC_SEX", "BORO", "PERP_SEX", "LOCATION_DESC", "PERP_AGE_GROUP"]
    for column in check_columns:
        distinct_values = shooting_data.select(column).distinct()
        print(f"Distinct values in {column}:")
        print(distinct_values.show())

    no_date_column = ["VIC_RACE", "OCCUR_TIME", "INCIDENT_KEY", "VIC_AGE_GROUP", "LATITUDE", "PERP_RACE", "STATISTICAL_MURDER_FLAG", "LONGITUDE", "VIC_SEX", "BORO", "PERP_SEX", "LOCATION_DESC", "PRECINCT", "PERP_AGE_GROUP"]
    for column in no_date_column:
        for key, value in null_value_mapping.items():
            shooting_data = shooting_data.withColumn(column, when(col(column) == key, value).otherwise(col(column))).collect()

    race_columns = ["VIC_RACE", "PERP_RACE" ]
    for column in race_columns:
        for key, value in race_mapping.items():
            shooting_data = shooting_data.withColumn(column, when(col(column) == key, value).otherwise(col(column))).collect()

    gender_columns = ["PERP_SEX", "VIC_SEX"]
    for column in gender_columns:
        for key, value in gender_mapping.items():
            shooting_data = shooting_data.withColumn(column, when(col(column) == key, value).otherwise(col(column))).collect()

    for column in check_columns:
        distinct_values = shooting_data.select(column).distinct().collect()
        print(f"Distinct values after processing in {column}:")
        print(distinct_values.show())

    for column in check_columns:
        distinct_count = shooting_data.select(column).distinct().count().collect()
        print(f"Number of distinct values in {column}: {distinct_count}")
    
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, shooting_data_table_name, nypd_precincts, mode="overwrite")