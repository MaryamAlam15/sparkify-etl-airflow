from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_fact_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_fact_sql=load_fact_sql

    def execute(self, context):
        self.log.info('Loading Fact Table.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.load_fact_sql)
