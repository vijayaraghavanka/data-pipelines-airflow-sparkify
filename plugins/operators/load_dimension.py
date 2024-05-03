from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 truncate,
                 query,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.truncate=truncate
        self.query=query
        

    def execute(self, context):
        self.log.info(f"{self.table_name} - Dimension table loading started")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate:
            self.log.info(f"Clearing data from {self.table_name} table")
            redshift_hook.run(f"Truncate table {self.table_name}")

        redshift_hook.run(f"insert into {self.table_name} {self.query}")
        self.log.info(f"{self.table_name} Dimension table loading is completed")
