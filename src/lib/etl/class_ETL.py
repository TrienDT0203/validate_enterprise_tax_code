
class ETL:
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

        with open('./src/lib/etl/card_id_51798.sql','r') as file:
            query_str = file.read()
            query_str = query_str.format(update_time= f"'{_update_time}'")
            

        credentials = service_account.Credentials.from_service_account_file(_file_path)
        client = bigquery.Client( credentials=credentials, project=credentials.project_id)

        job = client.query(query_str).result()
        data = job.to_dataframe()

        return data

    # fetch data from local_db which is duck_db, sqlite, gg_sheet...
    def fetch_data_local_db(self, db_path):
        import sqlite3
        import pandas as pd 

        con = sqlite3.connect(db_path)
        
        data = pd.read_sql(sql= 'select * from company_tax_code', con= con)

        return data 
    
    def transform_data_from_web(self, _tax_code, _headers_list= None,_proxy_list= None):
            from bs4 import BeautifulSoup
            import requests
            import random

            _dict = {'name':'',
                    'id':'',
                    'address':'',
                    }
            
            count = 0
            while count<3:
                _url = f'http://masothue.com/Search/?q={_tax_code}&type={random.choice(['auto','enterpriseTax'])}'
                
                try:
                    _res = requests.get(_url, 
                                        headers= random.choice(_headers_list) if _headers_list else None, 
                                        timeout = 60*3,
                                        proxies={
                                                'http': _proxy_list[random.randint(1,len(_proxy_list))],
                                                'https': _proxy_list[random.randint(1,len(_proxy_list))],
                                                } if _proxy_list else None,
                                    )
                except Exception as e:
                    print(e)
                    continue
                
                if BeautifulSoup(_res.text).find('table', {'class':'table-taxinfo'}):
                    break
                else:
                    count+=1
            
            # extract data
            if BeautifulSoup(_res.text).find('table', {'class':'table-taxinfo'}):
                #name
                _dict['name'] =  BeautifulSoup(_res.text).find('th',{"itemprop":"name"}).text.strip()
                #tax_code
                _dict['id'] = str(_tax_code)
                #address
                _dict['address'] = BeautifulSoup(_res.text).find('td',{"itemprop":"address"}).text.strip()
            else:
                _dict['name'] = 'No data'
                _dict['id'] = str(_tax_code)
                _dict['address'] = 'No data'
            return _dict

    def transform_data_from_api(self,_tax_code, _headers_list= None,_proxy_list= None):
        import requests
        import json

        response = requests.get(f'https://api.vietqr.io/v2/business/{_tax_code}',
                                        timeout= 10*60 )
        if response.status_code == 200:
            pass
        else:
            return {'name':'No data', 'id': str(_tax_code), 'address':'No data'} 

        if response.json()['desc'].lower().startswith('success'):
            try:
                data = eval(response.text)
            except:
                data = json.loads(response.text)
        
            _result = data['data']
        
            return _result

    
        return {'name':'No data', 'id': str(_tax_code), 'address':'No data'}        
        
    
    def update_data_local_db(self, db_path, _data_to_update: tuple):
        import sqlite3
        # import pandas as pd 

        conn = sqlite3.connect(db_path)
        
        try:
            cursor = conn.cursor()
            cursor.execute('insert into company_tax_code values (?,?,?)', _data_to_update)
            conn.commit()
            conn.close()
        except Exception as e:
            conn.rollback()
            conn.close()
            return {'status': 'error', 'message': e}
        finally:
            conn.close()

        return {'status': 'success', 'message': 'Data updated successfully'}

    # export data to csv
    def write_data_to_csv_file(self, _data:str, file_name, _file_path= './data/process'):

        import os 

        if not os.path.exists(_file_path):
            os.makedirs(_file_path, exist_ok= True)

            if not os.path.isfile(_file_path + '/' + file_name):
                with open(_file_path + '/' + file_name, 'a', newline= '') as f:
                    f.write('id,name,update_time,user_id \n')
                    f.close()
        
        try:
            with open(_file_path + '/' + file_name, 'a') as f:
                f.write(_data + '\n')
                f.close()
                
        except Exception as e:
            return {'status': 'error', 'message': e}

        return {'status': 'success', 'message': 'Data exported successfully'}


   