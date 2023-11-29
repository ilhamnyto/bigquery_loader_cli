import pandas as pd
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account


class BQTable:
    def __init__(self, filename: str) -> None:
        credentials = service_account.Credentials.from_service_account_file(filename)
        self.client = bigquery.Client(credentials=credentials)

    def set_table(self, project_name: str, dataset_id: str, table_id: str) -> bigquery.Table:
        return self.client.get_table(f"{project_name}.{dataset_id}.{table_id}")
    
    def create_table(self, project_name: str, dataset_id: str, table_id: str, schemas: list) -> bigquery.Table:
        dataset_ref = self.client.dataset(dataset_id, project=project_name)
        self.client.create_dataset(dataset_ref, exists_ok=True)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schemas)
        self.client.create_table(table, exists_ok=True)  
        
        return self.set_table(project_name, dataset_id, table_id)
    
    def truncate_insert(self, table: bigquery.Table, data: pd.DataFrame) -> None:
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=False,
                source_format=bigquery.SourceFormat.CSV,
            )
            self.client.load_table_from_dataframe(data, table, job_config=job_config)
        except Exception as e:
            print(e)

    def insert_append(self, data: pd.DataFrame, table: bigquery.Table) -> None:
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False,
                source_format=bigquery.SourceFormat.CSV,
            )
            
            self.client.load_table_from_dataframe(data, table, job_config=job_config)
        except Exception as e:
            print(e)
        
def BQTableLoader(filename: str) -> BQTable:
    return BQTable(filename)