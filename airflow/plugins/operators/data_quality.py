from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 postgres_conn_id='postgres',
                 sql_stmts=(),
                 result_checkers=(),
                 *args, **kwargs):
        if len(sql_stmts) != len(result_checkers):
            raise Exception('sql_stmts and result_checkers must have the same number of elements.')
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_stmts = sql_stmts
        self.result_checkers = result_checkers
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        for sql_stmt, result_checker in zip(self.sql_stmts, self.result_checkers):
            records = postgres.get_records(sql_stmt)
            if not result_checker(records):
                raise ValueError('Data quality check failed. SQL: {}'.format(sql_stmt))
            self.log.info("Passed Quality Check: '{}'.".format(sql_stmt))