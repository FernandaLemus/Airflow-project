from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook 



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
            COPY {}
            FROM '{}'
            JSON 'auto ignorecase'
            access_key_id '{}'
            secret_access_key '{}';
            """ 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials ="",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        test = BaseHook.get_connection('aws_credentials') 
        print(test.get_extra())
        hook = S3Hook(self.aws_conn_id, verify=False)
        credentials = hook.get_credentials(region_name = 'us-west-2')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('StageToRedshiftOperator sucessfully connected')
        
        self.log.info('Deleting data from destination Redshift table')
        redshift.run(f'DELETE FROM {self.table}')
        
        self.log.info('Copying data from s3 to Redshift')
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, 
            s3_path,
            credentials.access_key,
            credentials.secret_key )
        redshift.run(formatted_sql)
        
        self.log.info('Data into Redshift')
        