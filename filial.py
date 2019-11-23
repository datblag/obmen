# -*- encoding: utf-8 -*-
import logging
from config import filial_config,filial_logname, filial_logname_debug, filial_logname_error, filial_tovar_group_id, \
    filial_tovar_group_name, filial_firma_white_list, filial_sklad_white_list
from sql import SqlClient
from wsdl import WsdlClient
from prihod import load_prihod_filial
from rashod import load_rashod_filial
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




#TODO сделать загрузку складов



#загрузка регионов
#get_region_groups_filial(cursor, wsdl_client=wsdl_client)

#загрузка клиентов
#get_client_groups_filial(wsdl_client=wsdl_client, prm_cursor=cursor)
# TODO убрать z в инн клиента

# cursor.execute('''
# 		select top 5000 closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
# 		SC4014.SP5011 as firma, SC172.SP573 as client, sc55.SP8452 as sklad, SP9324 as idartmarket,
# 		_1sjourn.iddoc,0 as zatr_nashi,0 as zatr_post,0 as naedinicu,
#         '0' as isreturn, _1sjourn.iddoc as OBJID
# 		from DH1582 as dh WITH (NOLOCK)
#         left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc
# 		left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
# 		left join SC172 WITH (NOLOCK) on SP1555 = SC172.id
# 		left join SC55 WITH (NOLOCK) on SP1565 = SC55.id
# 		where (CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) >='2019-01-01')
# ''')
#
# rows_prihod = cursor.fetchall()
# for row_prihod in rows_prihod:
#     if not row_prihod['firma'].strip() in filial_firma_white_list:
#         continue
#     if not row_prihod['sklad'].strip() in filial_sklad_white_list:
#         continue
#     logging.warning(row_prihod)
#     load_prihod_filial(cursor, wsdl_client, row_prihod)




cursor.execute('''
            SELECT  top 1000 closed, CAST(LEFT(Date_Time_IDDoc, 8) as DateTime) as datedoc,docno,
            SC4014.SP5011 as firma,
            SC172.SP573 as client,
            sc55.SP8452 as sklad,
            SP9325 as idartmarket,
            '' as agent,
            '' as expeditor,
            '' as expeditorname,
            _1sjourn.iddoc, _1sjourn.iddoc as OBJID FROM DH1611 as dh WITH (NOLOCK)
            left join _1sjourn WITH (NOLOCK) on dh.iddoc=_1sjourn.iddoc
            left join SC4014 WITH (NOLOCK) on SP4056=SC4014.id
            left join SC172  WITH (NOLOCK) on SP1583 = SC172.id
            left join sc55 WITH (NOLOCK) on SP1593 = sc55.id
			where CAST(LEFT(Date_Time_IDDoc, 8) as DateTime)>='2019-01-01'
''')

rows_rashod = cursor.fetchall()
for row_rashod in rows_rashod:
    if not row_rashod['firma'].strip() in filial_firma_white_list:
        continue
    if not row_rashod['sklad'].strip() in filial_sklad_white_list:
        continue
    #logging.warning(row_rashod)
    load_rashod_filial(cursor, wsdl_client, row_rashod)






# TODO загрузка строк приходов
# TODO загрузка измененых приходов



#загрузка остатков склада
# ostatki.load_ostatki_sklad_filial(wsdl_client, cursor, filial_config['firma_white_list'],
#                                     filial_config['sklad_white_list'])







# TODO Завести в 1С цену приобретения