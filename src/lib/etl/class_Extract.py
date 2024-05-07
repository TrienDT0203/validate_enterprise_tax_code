class Extract:
    def __init__(self):
        pass

    def fetch_data_from_metabase_api(self, card_id, conditions):
        import requests
        import pandas as pd

        res = requests.get(url= 'https://ep.ahamove.com/bi/v1/metabase_card?cardid=' + card_id ,
                           params= conditions,
                           timeout= 5 *60,
                           )

        try:
            df = pd.DataFrame(res.json())
            return df
        except Exception as e:
            return e

    # fetch data from bigquery
    def fetch_data_from_bigquery(self,_file_path, _update_time):
        from google.cloud import bigquery
        from google.oauth2 import service_account

        with open(_file_path,'r') as file:
            query_str = file.read()
            query_str = file.format(update_time= f"'{_update_time}'")


        credentials = service_account.Credentials.from_service_account_file(_file_path)
        client = bigquery.Client( credentials=credentials, project=credentials.project_id)

        job = client.query(query_str).result()
        data = job.to_dataframe()

        return data

    # fetch data from local_db which is duck_db, sqlite, gg_sheet...
    def fetch_data_local_db(self, db_path, query_path):
        import sqlite3
        import pandas as pd 

        con = sqlite3.connect(db_path)
        
        data = pd.read_sql(sql= 'select * from company_tax_code', con= con)

        return data 
    
    