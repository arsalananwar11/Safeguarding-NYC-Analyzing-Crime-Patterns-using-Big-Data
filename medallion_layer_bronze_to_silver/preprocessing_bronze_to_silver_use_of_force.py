import sys
sys.path.append("..")

from snowflake.snowpark import Session
from snowflake.snowpark.functions import when, col, count, to_date
from datetime import date
from helpers import SnowflakeHelper
import json
import os

borough_mapping = {
    "PBBX": "BRONX", 
    "PBSI": "STATEN ISLAND", 
    "PBMN": "MANHATTAN", 
    "PBMS": "MANHATTAN",
    "PBBN": "BROOKLYN", 
    "PBBS": "BROOKLYN", 
    "PBQS": "QUEENS", 
    "PBQN": "QUEENS"
}

class UseOfForceProcessor():
    def get_distinct_counts(self, dataframe, column_name):
        return dataframe.groupBy(column_name).agg(count("*").alias("count")).sort("count", ascending=False)

    def process_uof_incidents_data(self, use_of_force_incidents):
        use_of_force_incidents =use_of_force_incidents.drop(["_AIRBYTE_RAW_ID", "_AIRBYTE_EXTRACTED_AT", "_AIRBYTE_META", "BASISFORENCOUNTER", "YEARMONTHSHORT"])
        use_of_force_incidents = use_of_force_incidents.filter(col("PATROL BOROUGH").is_not_null()).collect()

        mapping_expr = when(col("PATROL BOROUGH") == 'PBBX', 'BRONX')
        for key, value in borough_mapping.items():
            mapping_expr = mapping_expr.when(col("PATROL BOROUGH") == key, value)

        use_of_force_incidents = use_of_force_incidents.withColumn("PATROL BOROUGH", mapping_expr.otherwise(col("PATROL BOROUGH")))
        columns_to_check = ["FORCETYPE", "PATROL BOROUGH"]
        for col_name in columns_to_check:
            distinct_counts = self.get_distinct_counts(use_of_force_incidents, col_name)
            print(f"Distinct counts after processing for {col_name}:")
            print(distinct_counts.show())

        return use_of_force_incidents

    def process_uof_subjects_data(self, use_of_force_subjects):
        use_of_force_subjects =use_of_force_subjects.drop(["_AIRBYTE_RAW_ID", "_AIRBYTE_EXTRACTED_AT", "_AIRBYTE_META", "SUBJECT INJURED", "SUBJECT INJURY LEVEL", "SUBJECT USED FORCE"])

        columns_to_check = ["SUBJECT GENDER", "AGE", "SUBJECT RACE"]  # List of columns you want to check
        for col_name in columns_to_check:
            distinct_counts = self.get_distinct_counts(use_of_force_subjects, col_name)
            print(f"Distinct counts for {col_name}:")
            print(distinct_counts.show())

        use_of_force_subjects = use_of_force_subjects.withColumn("SUBJECT GENDER", when(col("SUBJECT GENDER") == "UNK", None).otherwise(col("SUBJECT GENDER"))).collect()
        use_of_force_subjects = use_of_force_subjects.withColumn(
            "SUBJECT RACE", 
            when(col("SUBJECT RACE") == "ASIAN", "ASIAN/PACIFIC ISLANDER")
            .when(col("SUBJECT RACE") == "AMER INDIAN", "AMERICAN INDIAN/ALASKAN NATIVE")
            .otherwise(col("SUBJECT RACE"))
        )

        columns_to_check = ["SUBJECT GENDER", "SUBJECT RACE"]  # List of columns you want to check
        for col_name in columns_to_check:
            distinct_counts = self.get_distinct_counts(use_of_force_subjects, col_name)
            print(f"Distinct counts after processing for {col_name}:")
            print(distinct_counts.show())

        return use_of_force_subjects


if __name__ == '__main__':
    schema_name = 'SAFEGUARDING_NYC_SCHEMA_SILVER'
    uof_incidents_table_name = 'USE_OF_FORCE_INCIDENTS'
    uof_subjects_table_name = 'USE_OF_FORCE_SUBJECTS'
    uof_processor = UseOfForceProcessor()
    snowflake_helper = SnowflakeHelper()
    snowflake_config = './../helpers/snowflake_config.json'
    session = snowflake_helper.create_snowpark_session(snowflake_config)
    session.use_schema("SAFEGUARDING_NYC_SCHEMA_BRONZE")

    use_of_force_incidents = session.table('USE_OF_FORCE_INCIDENTS')
    use_of_force_incidents = uof_processor.process_uof_incidents_data(use_of_force_incidents)
    snowflake_helper.save_data_in_snowflake(session, schema_name, uof_incidents_table_name, use_of_force_incidents, mode="overwrite")

    use_of_force_subjects = session.table('USE_OF_FORCE_SUBJECTS')
    use_of_force_subjects = uof_processor.process_uof_subjects_data(use_of_force_subjects)
    snowflake_helper.save_data_in_snowflake(session, schema_name, uof_subjects_table_name, use_of_force_subjects, mode="overwrite")
