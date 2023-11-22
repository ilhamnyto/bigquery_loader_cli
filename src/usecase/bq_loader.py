import aiomysql
import pandas as pd
from google.cloud import bigquery
from src.pkg.bq import BQTableLoader
from src.adapter.mysql import MySQLConnection, Connection

class Loader():
    def __init__(self) -> None:
        self.mysql = MySQLConnection()

    async def run(self):
        try:
            pool = await self.mysql.create_pool()
            bqloader = BQTableLoader()
            db = 'work'
            table = 'master_short_analysis'
            project_name = 'learngo-401304'
            dataset_id = 'testing'
            query = """
                    select * from master_short_analysis limit 2
            """

            schema = await self.get_source_table_schema(pool, db, table)
            data = await self.get_data(self.mysql, pool, query)

            bqtable = bqloader.create_table(project_name=project_name, dataset_id=dataset_id, table_id=table, schemas=schema)
            bqloader.insert_append(table=bqtable, data=data)
            

        except Exception as e:
            print(e)
 
    async def get_source_table_schema(self, pool: aiomysql.Pool, db_name: str, table_name: str) -> dict:
        query = """SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = '{db_name}' AND table_name = '{table_name}'""".format(db_name=db_name, table_name=table_name)
        _, result = await self.mysql.get_query(pool, query)
        data = [list(a) for a in result]
        for row in data:
            if row[1] == 'int':
                row[1] = 'INT64'
            elif row[1] == 'numeric':
                row[1] = 'NUMERIC'
            elif row[1] == 'bigint':
                row[1] = 'BIGUMERIC'
            elif row[1] == 'float':
                row[1] = 'FLOAT64'
            elif row[1] == 'datetime':
                row[1] = 'DATETIME'
            elif row[1] == 'date':
                row[1] = 'DATE'
            elif row[1] == 'bool':
                row[1] = 'BOOLEAN'
            elif row[1] == 'varchar':
                row[1] = 'STRING'

        schema = [
            bigquery.SchemaField(column, types) for column, types in data
        ]

        return schema
    
    async def get_data(self, client: Connection, pool: aiomysql.Pool, query: str) -> pd.DataFrame:
        columns_name, result = await client.get_query(pool, query)
        data = [{columns_name[i] : row[i] for i in range(len(columns_name))} for row in result]
        df = pd.DataFrame(data)
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].astype('string')

        return df
            
def BigQueryLoader() -> Loader:
    return Loader()