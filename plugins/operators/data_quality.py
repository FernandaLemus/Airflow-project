from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table_names = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        self.log.info('Connected to Redshift successfully')
        
        for table in self.table_names:
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            self.log.info(f'DataQualityOperator validating row count for {table}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'DataQualityOperator check failed. {table} retuned no results')
        
        id_checks=[
            {'table': 'users',
             'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null",
             'expected_result': 0},
            {'table': 'songs',
             'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
             'expected_result': 0},
            {'table': 'artist',
             'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null",
             'expected_result': 0}]
        
        for check in id_checks:
            records = redshift.get_records(check['check_sql'])[0]
            self.log.info('DataQualityOperator looking for Null ids')
            
            if records[0] != check['expected_result']:
                raise ValueError(f"Data quality check failed. {check['table']} contains null in id column,got {records[0]} instead")
            else:
                self.log.info('DataQualityOperator did not find null ids')
