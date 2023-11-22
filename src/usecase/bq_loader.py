import os
import typer
import aiomysql
import inquirer
import pandas as pd
from rich import print
from yaspin import yaspin
from google.cloud import bigquery
from src.pkg.bq import BQTableLoader
from yaspin.spinners import Spinners
from src.adapter.mysql import MySQLConnection, Connection

class Loader():
    def __init__(self, database: str, credentials: dict) -> None:
        self.credentials = credentials
        self.spinner = yaspin(Spinners.noise, text="Loading..")
        if database.lower() == 'mysql':
            self.conn = MySQLConnection(credentials)

    async def run(self):
        try:
            self.spinner.start()
            pool = await self.conn.create_pool()
            self.spinner.stop()
            
            self.spinner.start()
            tables = await self.get_table_names(pool)
            self.spinner.stop()
            selected_tables = inquirer.list_input("Choose source database?", choices=tables)
            
            schema = await self.get_source_table_schema(pool, self.credentials['db'], selected_tables)
            data = await self.get_data(self.conn, pool, selected_tables)
            print(data.dtypes)

            sa_option = inquirer.list_input("Have you download your BigQuery project service account?", choices=['Yes.', 'Not yet.'])

            if sa_option.lower() == 'yes.':
                print(f"[bold yellow]What's your service account filename: [/bold yellow]", end=' ')
                filename = input()
                if not os.path.exists(filename):
                    print(f"{filename} is not found in the root folder. 🙏")
                                
                print(f"[bold yellow]Project name: [/bold yellow]", end=' ')
                project_name = input()
                print(f"[bold yellow]Dataset name: [/bold yellow]", end=' ')
                dataset_id = input()

                self.spinner.start()
                bqloader = BQTableLoader(filename)
                bqtable = bqloader.create_table(project_name=project_name, dataset_id=dataset_id, table_id=selected_tables, schemas=schema)
                bqloader.truncate_insert(table=bqtable, data=data)
                self.spinner.stop()
            else:
                print("You need to download you project service account first and place in the root folder. ✌️")

        except Exception as e:
            self.spinner.stop()
            print(f"💥 {repr(e)}")
 
    async def get_source_table_schema(self, pool: aiomysql.Pool, db_name: str, table_name: str) -> dict:
        query = """SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = '{db_name}' AND table_name = '{table_name}'""".format(db_name=db_name, table_name=table_name)
        _, result = await self.conn.get_query(pool, query)
        data = [list(a) for a in result]
        for row in data:
            if row[1] == 'int':
                row[1] = 'INT64'
            elif row[1] == 'numeric':
                row[1] = 'NUMERIC'
            elif row[1] == 'bigint':
                row[1] = 'BIGNUMERIC'
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
    
    async def get_table_names(self, pool: aiomysql.Pool) -> list:
        query = """
                show tables
            """
        _, result = await self.conn.get_query(pool, query)

        return [a[0] for a in list(result)]
    
    async def get_data(self, client: Connection, pool: aiomysql.Pool, table: str) -> pd.DataFrame:
        query = f"select * from {table} limit 5"
        columns_name, result = await client.get_query(pool, query)
        data = [{columns_name[i] : row[i] for i in range(len(columns_name))} for row in result]
        df = pd.DataFrame(data)
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].astype('string')

        return df
            
def BigQueryLoader(database: str, credentials: dict) -> Loader:
    return Loader(database=database, credentials=credentials)