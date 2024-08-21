from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_dimension_table_insert = """
    INSERT INTO {} {}
    """
    load_dimension_table_truncate = """
    TRUNCATE TABLE {} 
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Started LoadDimensionOperator')

        truncate = LoadDimensionOperator.load_dimension_table_truncate.format(self.table)
        redshift.run(truncate)

        insert = LoadDimensionOperator.load_dimension_table_insert.format(self.table, self.sql)
        redshift.run(insert)

