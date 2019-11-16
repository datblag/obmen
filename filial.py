# -*- encoding: utf-8 -*-
import logging
from config import filial_config,filial_logname, filial_logname_debug, filial_logname_error, filial_tovar_group_id, \
    filial_tovar_group_name
from sql import SqlClient
from wsdl import WsdlClient
import logs
from hdb import get_region_groups_filial, get_client_groups_filial


import ostatki

logs.run(filial_logname, filial_logname_debug, filial_logname_error)



wsdl_client = WsdlClient(filial_config['server_1c'])

client = wsdl_client.client

sql_client = SqlClient(filial_config['sql'])
cursor = sql_client.cursor
conn = sql_client.conn


#загрузка корневой группы товаров
tovar_group_list = []
nom_group = wsdl_client.nomenklatura_type(code='', name=filial_tovar_group_name,
                                          id=filial_tovar_group_id, idparent='')
tovar_group_list.append(nom_group)
arrayn_group = wsdl_client.arrayn_type(nomenklatura=tovar_group_list)
wsdl_client.client.service.load_nom_groups(arrayn_group, 1)



#загрузка регионов
#get_region_groups_filial(cursor, wsdl_client=wsdl_client)

#загрузка клиентов
#get_client_groups_filial(wsdl_client=wsdl_client, prm_cursor=cursor)

#загрузка остатков склада
# ostatki.load_ostatki_sklad_filial(wsdl_client, cursor, filial_config['firma_white_list'],
#                                    filial_config['sklad_white_list'])


#номенклатура SC84





