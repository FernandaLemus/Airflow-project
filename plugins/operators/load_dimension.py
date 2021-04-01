from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 query_dimension = "",
                 destination_table = "",
                 append_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_dimension = query_dimension
        self.destination_table = destination_table
        self.append_data = append_data
        


    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Succesfully connected to Redshift')
        
        if self.append_data == False:
            self.log.info(f'Deleting existing data from {self.destination_table}')
            redshift.run(f'DELETE FROM {self.destination_table};')
            
            self.log.info(f'Loading data to {self.destination_table}')
            redshift.run(f'INSERT INTO {self.destination_table} ({self.query_dimension});')
            self.log.info(f'Data loaded to {self.destination_table} table successfully')
        
        if self.append_data == True:
            self.log.info(f'Appending data to {self.destination_table}')
            redshift.run(f'INSERT INTO {self.destination_table} ({self.query_dimension});')
            self.log.info(f'Data loaded to {self.destination_table} table successfully')
            
        
