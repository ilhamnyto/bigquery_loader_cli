from src.adapter.mysql import MySQLConnection
from google.cloud import bigquery
import aiomysql

class Loader():
    def __init__(self) -> None:
        self.mysql = MySQLConnection()

    async def run(self):
        pool = await self.mysql.create_pool()

        schema = await self.get_source_table_schema(pool, 'work', 'master_short_analysis')
 
    async def get_source_table_schema(self, pool: aiomysql.Pool, db_name: str, table_name: str) -> dict:
        query = """SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = '{db_name}' AND table_name = '{table_name}'""".format(db_name=db_name, table_name=table_name)
        columns_name, result = await self.mysql.get_query(pool, query)
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
            

        


        

def BigQueryLoader() -> Loader:
    return Loader()