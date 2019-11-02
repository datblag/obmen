# -*- encoding: utf-8 -*-
import logging
from config import filial_config,filial_logname, filial_logname_debug, filial_logname_error
from sql import SqlClient
from wsdl import WsdlClient
import logs

logs.run(filial_logname, filial_logname_debug, filial_logname_error)



wsdl_client = WsdlClient(filial_config['server_1c'])

client = wsdl_client.client

sql_client = SqlClient(filial_config['sql'])
cursor = sql_client.cursor
conn = sql_client.conn


#номенклатура SC84





