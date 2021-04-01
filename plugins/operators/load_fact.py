from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 query_fact = "",
                 destination_table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.query_fact = query_fact

        


    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Succesfully connected to Redshift')
        
        self.log.info(f'Appending data to {self.destination_table}')
        redshift.run(f'INSERT INTO {self.destination_table} {self.query_fact}')
        self.log.info(f'Data loaded to {self.destination_table} table successfully')
        
        

                      
