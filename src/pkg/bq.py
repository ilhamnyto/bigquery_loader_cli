import pandas as pd
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('credentials.json')

class BQTable:
    def __init__(self) -> None:
        self.client = bigquery.Client(credentials=credentials)

    def set_table(self, project_name: str, dataset_id: str, table_id: str) -> bigquery.Table:
        return self.client.get_table(f"{project_name}.{dataset_id}.{table_id}")
    
    def create_table(self, project_name: str, dataset_id: str, table_id: str, schemas: List[bigquery.SchemaField]) -> bigquery.Table:
        datataset_ref = self.client.dataset(dataset_id=dataset_id, project=project_name)
        table_ref = datataset_ref.table(table_id=table_id)
        table = bigquery.Table(table_ref=table_ref, schema=schemas)
        self.client.create_table(table, exists_ok=True)

        return self.set_table(project_name=table.project, dataset_id=table.dataset_id, table_id=table.table_id)
    
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
        
def BQTableLoader() -> BQTable:
    return BQTable()