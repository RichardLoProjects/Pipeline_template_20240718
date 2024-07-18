'''Data pipeline template to help describe the ETL process.'''
import pandas as pd
import os
from dotenv import load_dotenv # type: ignore
import psycopg2 as psql # type: ignore
import warnings


class EnvSecrets:
    def __init__(self) -> None:
        '''Fetch all sensitive data and load into variables for later use.'''
        load_dotenv()
        self.db_name = os.getenv('DATABASE_NAME')
        self.db_host = os.getenv('DATABASE_HOST')
        self.db_port = int(os.getenv('DATABASE_PORT'))
        self.sql_user = os.getenv('SQL_USERNAME')
        self.sql_pass = os.getenv('SQL_PASSWORD')
        self.schema = os.getenv('SQL_SCHEMA')

class DatabaseConnection:
    def __init__(self, credentials:EnvSecrets) -> None:
        '''Open database connection.'''
        self.connection = psql.connect(
            database = credentials.db_name
            , host = credentials.db_host
            , port = credentials.db_port
            , user = credentials.sql_user
            , password = credentials.sql_pass
        )
        self.cursor = self.connection.cursor()
    def close(self) -> None:
        '''Close database connection.'''
        try:
            self.connection.close()
        except:
            pass

class DataPipeline:
    def __init__(
            self
            , data_source:str
            , data_target:str
            , primary_key:str
            , connection:DatabaseConnection
            , schema:str
            , tablename:str
        ) -> None:
        self.df = pd.DataFrame()
        self.source = data_source
        self.target = data_target
        self.pk = primary_key
        self.connection = connection
        self.schema = schema
        self.tablename = tablename
    def extract(self) -> None:
        '''Extract data from the data source and populate dataframe.'''
        ### Implement populate-dataframe here
        code_to_read_from_db = '''
        self.connection.cursor.execute(f'SELECT * FROM {self.schema}.{self.tablename}')
        data = self.connection.cursor.fetchall()
        cols = [desc[0] for desc in self.connection.cursor.description]
        self.df = pd.DataFrame(data, columns=cols)
        '''
    def transform(self) -> None:
        '''Clean and sort data into a desirable format. Consider normal forms: 1nf 2nf 3nf.'''
        ### Implement clean-data here
        pass
    def load(self) -> None:
        '''Load data into database.'''
        code_to_write_to_db = """
        create_table_sql = f'''
CREATE TABLE IF NOT EXISTS {self.schema}.{self.tablename} (
    {self.pk} int PRIMARY KEY
    , another_column int
);'''
        self.connection.cursor.execute(create_table_sql)
        ### Implement row-insertion here
        self.connection.connection.commit()
        """
    def email_sos(self, error_message:str) -> None:
        '''Notify errors remotely.'''
        ### Implement email-notifications here.
        pass


def main() -> None:
    warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy connectable.*')
    try:
        data_source = 'api-url or csv-file here'
        data_target = 'file-path if storing locally'
        tablename = 'your tablename'
        primary_key = 'unique column'
        sensitive_data = EnvSecrets()
        connection = DatabaseConnection(sensitive_data)
        pipeline = DataPipeline(
            data_source
            , data_target
            , primary_key
            , connection
            , sensitive_data.schema
            , tablename
        )
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
    except Exception as error:
        pipeline.email_sos(error)
    finally:
        connection.close()

if __name__ == '__main__':
    main()
