class Load:
    def __init__(self):
        pass

    # update data to sqlite 
    def update_data_local_db(self, db_path, _table_name, _data_to_update):
        import sqlite3
        # import pandas as pd 

        con = sqlite3.connect(db_path)
        
        try:
            _data_to_update.to_sql( _table_name
                                , con
                                , if_exists='append'
                                , index=False
                                )
        except Exception as e:
            return {'status': 'error', 'message': e}
        finally:
            con.close()

        return {'status': 'success', 'message': 'Data updated successfully'}

    # export data to csv
    def export_data(self, _data, _file_path, file_name):
        # import pandas as pd
        import os 

        if not os.path.exists(_file_path):
            os.makedirs(_file_path, exist_ok= True)
        
        try:
            _data.to_csv(_file_path + '/' + file_name, index=False)
        except Exception as e:
            return {'status': 'error', 'message': e}

        return {'status': 'success', 'message': 'Data exported successfully'}


   