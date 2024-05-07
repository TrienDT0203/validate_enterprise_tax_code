class Transform:
    def __init__(self) -> None:
         pass

    def transform_data_from_web(self, _tax_code, _headers_list,_proxy_list):
            from bs4 import BeautifulSoup
            import requests
            import random

            _dict = {'name':'',
                    'id':'',
                    'address':'',
                    }
            
            count = 0
            while count<5:
                _url = f'http://masothue.com/Search/?q={_tax_code}&type=auto'
                
                try:
                    _res = requests.get(_url, 
                                        headers= random.choice(_headers_list), 
                                        timeout = (60,5),
                                        proxies={
                                                'http': _proxy_list[random.randint(1,len(_proxy_list))],
                                                'https': _proxy_list[random.randint(1,len(_proxy_list))],
                                                }
                                    )
                except Exception as e:
                    count-=1
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

    def transform_data_from_api(self,_tax_code, _headers_list,_proxy_list):
        import requests
        import json

        response = requests.get(f'https://api.vietqr.io/v2/business/{_tax_code}',
                                timeout= 10*60 )

        if response.json()['desc'] == 'Success - Thành công':
            try:
                data = eval(response.text)
            except:
                data = json.loads(response.text)
        
        _result = response.json()['data']

        if not _result:
            _result = self.extract_data_web(_tax_code, _headers_list,_proxy_list)
                
            return _result