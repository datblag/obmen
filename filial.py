# -*- encoding: utf-8 -*-
import logging
from config import filial_config,filial_logname, filial_logname_debug, filial_logname_error
from sql import SqlClient
from wsdl import WsdlClient
import logs

import ostatki

logs.run(filial_logname, filial_logname_debug, filial_logname_error)



wsdl_client = WsdlClient(filial_config['server_1c'])

client = wsdl_client.client

sql_client = SqlClient(filial_config['sql'])
cursor = sql_client.cursor
conn = sql_client.conn


#загрузка корневой группы товаров
tovar_group_list = []
nom_group = wsdl_client.nomenklatura_type(code='', name="!ТОВАРЫ ФИЛИАЛ ЗЕЯ",
                                          id='3b5374ea-0bcc-4d4d-bae6-971e2e748505', idparent='')
tovar_group_list.append(nom_group)
arrayn_group = wsdl_client.arrayn_type(nomenklatura=tovar_group_list)
wsdl_client.client.service.load_nom_groups(arrayn_group, 1)

#загрузка остатков склада
# ostatki.load_ostatki_sklad_filial(wsdl_client, cursor, filial_config['firma_white_list'],
#                                   filial_config['sklad_white_list'])


#номенклатура SC84





