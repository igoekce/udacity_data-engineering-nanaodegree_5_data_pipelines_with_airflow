#from airflow.hooks.postgres_hook import PostgresHook
#from airflow.models import BaseOperator
#from airflow.utils.decorators import apply_defaults

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 table='',
                 select_sql_stmt='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.select_sql_stmt = select_sql_stmt


    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # Postgres Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql_stmt = 'INSERT INTO {} ({})'.format(self.table, self.select_sql_stmt)

        # Load table
        self.log.info("Loading data into {}.".format(self.table))
        redshift.run(insert_sql_stmt)

