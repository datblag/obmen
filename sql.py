import pymssql
#from config import cb_sql_address,cb_sql_user_name,cb_sql_password,cb_sql_database

#import logs
import logging

#logs.run()


class SqlClient:
    def __init__(self, sql_config):
        self.conn = pymssql.connect(server=sql_config['sql_address'], user=sql_config['sql_user_name'],
                                    password=sql_config['sql_password'], database=sql_config['sql_database'])
        self.cursor = self.conn.cursor(as_dict=True)
        self.cursor.execute('''SET TRANSACTION ISOLATION LEVEL READ COMMITTED''')

