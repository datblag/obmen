from sql import cursor
from wsdl import client
import logging

def get_client_groups(prm_cursor, prm_id_list=''):
    hdb_type = client.get_type('ns3:hdb_element')
    hdb_array_type = client.get_type('ns3:hdb_array_element')

    list = []
    logging.info('Выборка клиентов')
    if prm_id_list == '':
        prm_cursor.execute('''

            select  SP56 as inn, sp4603 as kpp,descr,sp48 as parentname,sp4807 as idartmarket,SP6066 as regionid
            from sc46 where (isfolder<>'1')                       
                           ''')
    else:
        # print(prm_id_list)
        prm_cursor.execute('''

            select  SP56 as inn, sp4603 as kpp,descr,sp48 as parentname,sp4807 as idartmarket,SP6066 as regionid from sc46 where (isfolder<>'1')    
              and (sp4807 in (''' + prm_id_list + '''))''')
    logging.info('Выборка клиентов завершена')
    logging.info('Подготовка загрузки клиентов')
    row = cursor.fetchall()
    for r in row:
        if r['inn'].strip() == '':
            inn = '0000000000'
        else:
            inn = r['inn'].strip()
        nom = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent=r['regionid'].strip(), value1=inn,
                       value2=r['kpp'].strip(), value3=r['parentname'].strip())
        list.append(nom)

    hdb_array = hdb_array_type(hdb_array=list)
    logging.info('Загрузка клиентов начало')
    client.service.load_client_groups(hdb_array, 1)
    logging.info('Загрузка клиентов завершена')
