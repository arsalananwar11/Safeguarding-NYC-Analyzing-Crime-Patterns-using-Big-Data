import sys
sys.path.append("..")

from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import when, col, sum, concat, lit
from snowflake.snowpark.functions import expr, regexp_extract, to_date
from datetime import date
from helpers import SnowflakeHelper
import json
import os
import matplotlib.pyplot as plt
import pandas as pd

borough_mapping = {
    "PBBX": "BRONX", 
    "PBSI": "STATEN ISLAND", 
    "PBMN": "MANHATTAN", 
    "PBMS": "MANHATTAN",
    "PBBN": "BROOKLYN", 
    "PBBS": "BROOKLYN", 
    "PBQS": "QUEENS", 
    "PBQN": "QUEENS",
    "STATEN IS": "STATEN ISLAND"
}

null_value_mapping = {
    "(null)" : None,
    "NaN" : None,
    "(" : None,
    "NULL": None,
    "(nu": None, 
    "#N/A": None
}

race_mapping = {
    "ASIAN / PACIFIC ISLANDER": "ASIAN/PACIFIC ISLANDER",
    "ASIAN/PAC.ISL": "ASIAN/PACIFIC ISLANDER",
    "AMER IND": "AMERICAN INDIAN/ALASKAN NATIVE",
    "AMERICAN INDIAN/ALASKAN N": "AMERICAN INDIAN/ALASKAN NATIVE",
    "MIDDLE EASTERN/SOUTHWEST": "MIDDLE EASTERN/SOUTHWEST ASIAN"
}

gender_mapping = {
    "M": "MALE",
    "F": "FEMALE"
}

class SQFProcessor():
    def check_and_display_missing_value_percentages(self, sqf_data):
        total_rows = sqf_data.count()
        missing_counts = sqf_data.select([sum(col(c).isNull().cast("int")).alias(c) for c in sqf_data.columns])
        missing_percentages = missing_counts.select([(col(c) / total_rows * 100).alias(c) for c in missing_counts.columns])
        print(f'Missing percentages:\n{missing_percentages.show()}')

    def map_and_filter_patrol_borough_name(self, sqf_data):
        mapping_expr = when(col('STOP_LOCATION_PATROL_BORO_NAME') == 'PBBX', 'BRONX')
        for key, value in borough_mapping.items():
            mapping_expr = mapping_expr.when(col("STOP_LOCATION_PATROL_BORO_NAME") == key, value)

        preprocessed_sqf_df = sqf_data.withColumn("STOP_LOCATION_PATROL_BORO_NAME", mapping_expr.otherwise(col("STOP_LOCATION_PATROL_BORO_NAME"))).collect()
        preprocessed_sqf_df = preprocessed_sqf_df.filter(col('STOP_LOCATION_PATROL_BORO_NAME').isin(list(borough_mapping.values()))).collect()
        return preprocessed_sqf_df

    def map_and_filter_borough_name(self, preprocessed_sqf_df):
        mapping_expr = when(col('STOP_LOCATION_BORO_NAME') == 'PBBX', 'BRONX')
        for key, value in borough_mapping.items():
            mapping_expr = mapping_expr.when(col("STOP_LOCATION_BORO_NAME") == key, value)
        preprocessed_sqf_df = preprocessed_sqf_df.withColumn("STOP_LOCATION_BORO_NAME", mapping_expr.otherwise(col("STOP_LOCATION_BORO_NAME"))).collect()
        preprocessed_sqf_df = preprocessed_sqf_df.filter(col('STOP_LOCATION_BORO_NAME').isin(list(borough_mapping.values()))).collect()
        return preprocessed_sqf_df

    def format_date(self, preprocessed_sqf_df):
        sql_expr_for_date_conversion = """
            TO_DATE(STOP_FRISK_DATE)
        """
        preprocessed_sqf_df = preprocessed_sqf_df.withColumn("STOP_FRISK_DATE", expr(sql_expr_for_date_conversion)).collect()
        return preprocessed_sqf_df
    
    def check_and_display_distinct_values(self, preprocessed_sqf_df):
        distinct_values_cols_check = ['STOP_LOCATION_SECTOR_CODE', 'SUSPECT_SEX', 'ASK_FOR_CONSENT_FLG', 'WEAPON_FOUND_FLAG', 'MONTH2', 'FRISKED_FLAG',\
        'SUSPECT_HAIR_COLOR', 'SUSPECT_ARREST_OFFENSE', 'OFFICER_IN_UNIFORM_FLAG', 'JURISDICTION_CODE', 'SEARCHED_FLAG', 'SUSPECTS_ACTIONS_DECRIPTION_FLAG',\
        'SUPERVISING_OFFICER_RANK', 'OTHER_CONTRABAND_FLAG', 'SUSPECT_BODY_BUILD_TYPE', 'DEMEANOR_OF_PERSON_STOPPED', 'SUSPECT_ARRESTED_FLAG',\
        'SUSPECT_EYE_COLOR', 'CONSENT_GIVEN_FLG', 'STOP_WAS_INITIATED', 'ISSUING_OFFICER_RANK', 'OFFICER_EXPLAINED_STOP_FLAG',\
        'STOP_LOCATION_PATROL_BORO_NAME', 'STOP_LOCATION_BORO_NAME', 'YEAR2', 'SUSPECT_RACE_DESCRIPTION']
        for column in distinct_values_cols_check:
            distinct_count = preprocessed_sqf_df.select(column).distinct().count()
            print(f"Number of distinct values in {column}: {distinct_count}")

        for col in distinct_values_cols_check:
            distinct_values = preprocessed_sqf_df.select(col).distinct()
            print(f"Distinct values in {col}:")
            print(distinct_values.show())

    def handle_null_values(self, preprocessed_sqf_df):
        columns_to_convert_null_values = ['SUSPECT_WEIGHT', 'STOP_LOCATION_SECTOR_CODE', 'STOP_LOCATION_STREET_NAME', 'STOP_DURATION_MINUTES',\
        'SUSPECT_SEX', 'ASK_FOR_CONSENT_FLG', 'STOP_LOCATION_X', 'STOP_LOCATION_Y', 'DAY2', 'WEAPON_FOUND_FLAG', 'MONTH2', 'FRISKED_FLAG',\
        'SUSPECT_HAIR_COLOR', 'SUSPECT_ARREST_OFFENSE', 'OFFICER_IN_UNIFORM_FLAG', 'JURISDICTION_CODE', 'SEARCHED_FLAG', 'SUSPECTED_CRIME_DESCRIPTION',\
        'SUSPECT_HEIGHT', 'SUSPECTS_ACTIONS_DECRIPTION_FLAG', 'SUPERVISING_OFFICER_RANK', 'STOP_LOCATION_FULL_ADDRESS', 'OTHER_CONTRABAND_FLAG',\
        'SUSPECT_BODY_BUILD_TYPE', 'DEMEANOR_OF_PERSON_STOPPED', 'SUSPECT_ARRESTED_FLAG', 'SUSPECT_RACE_DESCRIPTION', 'SUSPECT_REPORTED_AGE',\
        'SUSPECT_EYE_COLOR', 'OBSERVED_DURATION_MINUTES', 'CONSENT_GIVEN_FLG', 'STOP_WAS_INITIATED', 'SEARCH_BASIS_OUTLINE_FLAG',\
        'ISSUING_OFFICER_RANK', 'OFFICER_EXPLAINED_STOP_FLAG', 'STOP_LOCATION_PRECINCT', 'SUSPECT_OTHER_DESCRIPTION', 'STOP_LOCATION_PATROL_BORO_NAME',\
        'STOP_LOCATION_BORO_NAME', 'YEAR2']

        for column in columns_to_convert_null_values:
            for key, value in null_value_mapping.items():
                preprocessed_sqf_df = preprocessed_sqf_df.withColumn(column, when(col(column) == key, value).otherwise(col(column)))

        preprocessed_sqf_df = preprocessed_sqf_df.collect()
        return preprocessed_sqf_df
    
    def drop_columns(self, sqf_data):
        columns_to_drop= ['_AIRBYTE_RAW_ID', '_AIRBYTE_EXTRACTED_AT', '_AIRBYTE_META', 'STOP_LOCATION_ZIP_CODE', 'ID_CARD_IDENTIFIES_OFFICER_FLAG', 'OFFICER_NOT_EXPLAINED_STOP_DESCRIPTION',\
                        'SUSPECTS_ACTIONS_CASING_FLAG', 'SUMMONS_ISSUED_FLAG', 'VERBAL_IDENTIFIES_OFFICER_FLAG', 'SEARCH_BASIS_ADMISSION_FLAG', 'SEARCH_BASIS_OTHER_FLAG', 'SEARCH_BASIS_CONSENT_FLAG',\
                        'BACKROUND_CIRCUMSTANCES_SUSPECT_KNOWN_TO_CARRY_WEAPON_FLAG', 'RECORD_STATUS_CODE', 'PHYSICAL_FORCE_RESTRAINT_USED_FLAG', 'PHYSICAL_FORCE_HANDCUFF_SUSPECT_FLAG',\
                        'OTHER_PERSON_STOPPED_FLAG', 'SUSPECTS_ACTIONS_PROXIMITY_TO_SCENE_FLAG', 'DEMEANOR_CODE', 'SUPERVISING_OFFICER_COMMAND_CODE', 'STOP_ID_ANONY', 'ISSUING_OFFICER_COMMAND_CODE',\
                        'LOCATION_IN_OUT_CODE', 'JURISDICTION_DESCRIPTION', 'PHYSICAL_FORCE_VERBAL_INSTRUCTION_FLAG', 'PHYSICAL_FORCE_OC_SPRAY_USED_FLAG', 'SEARCH_BASIS_HARD_OBJECT_FLAG', 'OTHER_WEAPON_FLAG', 'PHYSICAL_FORCE_WEAPON_IMPACT_FLAG',\
                    'PHYSICAL_FORCE_OTHER_FLAG', 'SUMMONS_OFFENSE_DESCRIPTION', 'SUSPECTS_ACTIONS_CONCEALED_POSSESSION_WEAPON_FLAG', 'SUSPECTS_ACTIONS_OTHER_FLAG',\
                    'PHYSICAL_FORCE_CEW_FLAG', 'FIREARM_FLAG', 'SEARCH_BASIS_INCIDENTAL_TO_ARREST_FLAG', 'SUSPECTS_ACTIONS_DRUG_TRANSACTIONS_FLAG', \
                    'BACKROUND_CIRCUMSTANCES_VIOLENT_CRIME_FLAG', 'STOP_LOCATION_APARTMENT', 'KNIFE_CUTTER_FLAG', 'KNIFE_CUTTER_FLAG',\
                    'PHYSICAL_FORCE_DRAW_POINT_FIREARM_FLAG', 'SHIELD_IDENTIFIES_OFFICER_FLAG',\
                    'SUSPECTS_ACTIONS_IDENTIFY_CRIME_PATTERN_FLAG', 'SUSPECTS_ACTIONS_LOOKOUT_FLAG']
    
        sqf_data = sqf_data.drop(*columns_to_drop).collect()
        return sqf_data


if __name__ == '__main__':
    sqf_processor = SQFProcessor()
    snowflake_helper = SnowflakeHelper()
    snowflake_config = './../helpers/snowflake_config.json'
    session = snowflake_helper.create_snowpark_session(snowflake_config)
    sqf_data = session.table('SQF')
    destination_table_name = 'SQF_DATA'
    destination_schema_name = 'SAFEGUARDING_NYC_SCHEMA_SILVER'

    sqf_processor.check_and_display_missing_value_percentages(sqf_data)
    sqf_data = sqf_processor.drop_columns(sqf_data)
    sqf_processor.check_and_display_missing_value_percentages(sqf_data)
    preprocessed_sqf_df = sqf_processor.map_and_filter_patrol_borough_name(sqf_data)
    preprocessed_sqf_df = sqf_processor.map_and_filter_borough_name(sqf_data) 
    preprocessed_sqf_df = sqf_processor.handle_null_values(sqf_data)
    sqf_processor.check_and_display_missing_value_percentages(preprocessed_sqf_df)
    sqf_processor.check_and_display_distinct_values(preprocessed_sqf_df)
    preprocessed_sqf_df = sqf_processor.format_date(preprocessed_sqf_df)
    snowflake_helper.save_data_in_snowflake(session, destination_schema_name, destination_table_name, preprocessed_sqf_df, mode="overwrite")