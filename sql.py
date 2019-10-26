import pymssql
from config import cb_sql_address,cb_sql_user_name,cb_sql_password,cb_sql_database

#import logs
import logging

#logs.run()


logging.warning('Соединение с sql сервером')

conn = pymssql.connect(server=cb_sql_address, user=cb_sql_user_name, password=cb_sql_password, database=cb_sql_database)
logging.warning('Соединение с sql сервером завершено')
cursor = conn.cursor(as_dict=True)

cursor.execute('''SET TRANSACTION ISOLATION LEVEL READ COMMITTED''')

