# -*- encoding: utf-8 -*-
import logging
from config import filial_config,filial_logname, filial_logname_debug, filial_logname_error, filial_tovar_group_id, \
    filial_tovar_group_name, filial_firma_white_list, filial_sklad_white_list, filial_object_white_list,\
    filial_base_code

from sql import SqlClient
from wsdl import WsdlClient
from prihod import load_prihod_filial, get_prihod_rows_filial
from rashod import load_rashod_filial, get_rashod_header
import logs
from tqdm import tqdm
from hdb import get_region_groups_filial, get_client_groups_filial
import ostatki


def main():
    k = 'авто'
    logs.run(filial_logname, filial_logname_debug, filial_logname_error)
    wsdl_client = WsdlClient(filial_config['server_1c'])
    client = wsdl_client.client
    sql_client = SqlClient(filial_config['sql'])
    cursor = sql_client.cursor
    conn = sql_client.conn

    if k == 'авто':
        while True:
            logging.warning('Выборка изменений')
            try:
                cursor.execute('''update  _1SUPDTS set DWNLDID='1122!!' where (DBSIGN = %s) and not (DWNLDID='1122!!')''', filial_base_code)
                conn.commit()
            except Exception as exception_name:
                logging.error(['ошибка при обновлении таблицы _1SUPDTS', exception_name])
            cursor.execute(
                '''SELECT  * from _1SUPDTS WITH (NOLOCK) where (DBSIGN = %s) and (DWNLDID='1122!!')''', filial_base_code)
            rows_delta = cursor.fetchall()
            for row_delta in tqdm(rows_delta):
                if not (row_delta['TYPEID'] in filial_object_white_list):
                    continue
                elif row_delta['TYPEID'] == 1582: #поступление
                    logging.warning(row_delta)
                    rows_prihod = get_prihod_rows_filial(cursor, row_delta['OBJID'])
                    for row_prihod in rows_prihod:
                        if not row_prihod['firma'].strip() in filial_firma_white_list:
                            continue
                        if not row_prihod['sklad'].strip() in filial_sklad_white_list:
                            continue
                        logging.warning(row_prihod)
                        load_prihod_filial(cursor, wsdl_client, row_prihod)
                elif row_delta['TYPEID'] == 1611: #реализация
                    logging.warning(row_delta)
                    rows_rashod = get_rashod_header(cursor, 1, 0, row_delta)
                    for row_rashod in rows_rashod:
                        if not row_rashod['firma'].strip() in filial_firma_white_list:
                            continue
                        if not row_rashod['sklad'].strip() in filial_sklad_white_list:
                            continue
                        logging.warning(row_rashod)
                        load_rashod_filial(cursor, wsdl_client, row_rashod)
                try:
                    cursor.execute('''delete from _1SUPDTS where (DBSIGN = %s) and (DWNLDID='1122!!') 
                                    and (OBJID=%s)''', (filial_base_code, row_delta['OBJID']))
                    conn.commit()
                    logging.info(';'.join(['Загружен объект', str(row_delta['OBJID']), str(row_delta['TYPEID'])]))
                except Exception as e:
                    logging.error(';'.join(['Ошибка загрузки объекта', str(row_delta['OBJID']), str(row_delta['TYPEID'])]))






    else:
        # загрузка корневой группы товаров
        tovar_group_list = []
        nom_group = wsdl_client.nomenklatura_type(code='', name=filial_tovar_group_name,
                                                  id=filial_tovar_group_id, idparent='')
        tovar_group_list.append(nom_group)
        arrayn_group = wsdl_client.arrayn_type(nomenklatura=tovar_group_list)
        wsdl_client.client.service.load_nom_groups(arrayn_group, 1)

        # TODO сделать загрузку складов

        # загрузка регионов
        # get_region_groups_filial(cursor, wsdl_client=wsdl_client)

        # загрузка клиентов
        # get_client_groups_filial(wsdl_client=wsdl_client, prm_cursor=cursor)
        # TODO убрать z в инн клиента


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
            # logging.warning(row_rashod)
            load_rashod_filial(cursor, wsdl_client, row_rashod)

        # TODO загрузка строк приходов
        # TODO загрузка измененых приходов

        # загрузка остатков склада
        # ostatki.load_ostatki_sklad_filial(wsdl_client, cursor, filial_config['firma_white_list'],
        #                                     filial_config['sklad_white_list'])

        # TODO Завести в 1С цену приобретения


if __name__ == '__main__':
    main()
