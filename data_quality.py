import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 tables=[],
                 table_column_tuples=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.table_column_tuples = table_column_tuples
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        for table_column_tuples in self.table_column_tuples:
            for table, field in table_column_tuples.items():
                null_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} where {field} is null")
                if len(null_records) < 1 or len(null_records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {table} returned NULL values in field {field}")
                logging.info(f"Data quality on table {table} and field {field}check passed with no NULL values")    
