from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name

    def execute(self, context):
        self.log.info('Data Quality Check started')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.table_name:
            records=redshift_hook.get_records(f"select count(*) from {table};")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"{table} table has no results")
            if records[0][0] < 1:
                raise ValueError(f"{table} table has no rows")
            self.log.info(f"Data quality check for {table} table passed with {records[0][0]} records")

            
