from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.query=query
        

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(f"insert into {self.table_name} {self.query}")
        self.log.info(f'{self.table_name} Fact table loading is completed')
