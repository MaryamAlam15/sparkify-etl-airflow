from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables_list=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.tables_list = tables_list
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Checking Data Quality.')

        for table in self.tables_list:
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if not (len(records) and len(records[0])):
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            if not num_records:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")

            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
