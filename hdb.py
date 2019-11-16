import logging
from config import filial_region_id, filial_region_name


def get_region_groups_filial(prm_cursor, prm_id_list='', wsdl_client=None):
    hdb_type = wsdl_client.client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.client.get_type('ns3:hdb_array_element')
    list_region = []
    nom_region = hdb_type(name=filial_region_name, id=filial_region_id, idparent='')
    list_region.append(nom_region)
    logging.info('Выборка регионов уровень 1')
    prm_cursor.execute('''select SP573 as idartmarket, descr,'' as idparent, '' as nameparent from
                       SC172 where ltrim(rtrim(parentid))='0' and isfolder=1''')
    logging.info('Выборка регионов уровень 1')
    logging.info('Подготовка регионов уровень 1')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent=filial_region_id)
        list_region.append(nom_region)

    logging.info('Выборка регионов уровень 2')
    prm_cursor.execute('''select SC172.SP573 as idartmarket,SC172.descr,scparent.SP573 as idparent,
                          scparent.descr as nameparent from SC172 left join SC172 as scparent on SC172.parentid=scparent.id
                          where (SC172.parentid in (select id from SC172 where isfolder=1 and ltrim(rtrim(parentid))='0'))
                          and SC172.isfolder=1''')

    logging.info('Выборка регионов уровень 2')
    logging.info('Подготовка регионов уровень 2')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region=hdb_type(name=r['descr'].strip(),id=r['idartmarket'].strip(),idparent=r['idparent'])
        list_region.append(nom_region)

    hdb_array=hdb_array_type(hdb_array=list_region)
    logging.info('Загрузка регионов начало')
    wsdl_client.client.service.load_hdb_elements(hdb_array,1,'region')
    logging.info('Загрузка регионов завершена')


def get_region_groups(prm_cursor, prm_id_list='', wsdl_client=None):
    hdb_type = wsdl_client.client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.client.get_type('ns3:hdb_array_element')
    list_region = []
    logging.info('Выборка регионов уровень 1')
    prm_cursor.execute('''select sp4807 as idartmarket, descr,'' as idparent, '' as nameparent from
                       sc46 where ltrim(rtrim(parentid))='0' and isfolder=1''')
    logging.info('Выборка регионов уровень 1')
    logging.info('Подготовка регионов уровень 1')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region=hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent='')
        list_region.append(nom_region)

    logging.info('Выборка регионов уровень 2')

    prm_cursor.execute('''select sc46.sp4807 as idartmarket,sc46.descr,scparent.sp4807 as idparent,
                          scparent.descr as nameparent from sc46 left join sc46 as scparent on sc46.parentid=scparent.id
                          where (sc46.parentid in (select id from sc46 where isfolder=1 and ltrim(rtrim(parentid))='0'))
                          and sc46.isfolder=1''')

    logging.info('Выборка регионов уровень 2')
    logging.info('Подготовка регионов уровень 2')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region=hdb_type(name=r['descr'].strip(),id=r['idartmarket'].strip(),idparent=r['idparent'])
        list_region.append(nom_region)

    hdb_array=hdb_array_type(hdb_array=list_region)
    logging.info('Загрузка регионов начало')
    wsdl_client.client.service.load_hdb_elements(hdb_array, 1, 'region')
    logging.info('Загрузка регионов завершена')






def send_clients(prm_clients_rows, wsdl_client, id_prefix=''):
    list_clients=[]
    for r in prm_clients_rows:
        if r['inn'].strip() == '':
            inn = '0000000000'
        else:
            inn = r['inn'].strip()
        nom = wsdl_client.hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(),
                                   idparent=r['regionid'].strip(),
                                   value1=id_prefix+inn, value2=r['kpp'].strip(), value3=r['parentname'].strip())
        list_clients.append(nom)

    hdb_array = wsdl_client.hdb_array_type(hdb_array=list_clients)
    logging.info('Загрузка клиентов начало')
    wsdl_client.client.service.load_client_groups(hdb_array, 1)
    logging.info('Загрузка клиентов завершена')



def get_client_groups_filial(wsdl_client = None, prm_cursor = None, prm_id_list = ''):
    logging.info('Выборка клиентов')
    if prm_id_list == '':
        prm_cursor.execute('''

            select  SP9312 as inn, SP9313 as kpp, descr, '' as parentname, SP573 as idartmarket, SP9314 as regionid
            from SC172 where (isfolder<>'1')                       
                           ''')
    else:
        # print(prm_id_list)
        prm_cursor.execute('''
            select  SP56 as inn, sp4603 as kpp,descr,sp48 as parentname,sp4807 as idartmarket,SP6066 as regionid from
             sc46 where (isfolder<>'1') and (sp4807 in (''' + prm_id_list + '''))''')
    logging.info('Выборка клиентов завершена')
    logging.info('Подготовка загрузки клиентов')
    row = prm_cursor.fetchall()
    send_clients(row, wsdl_client=wsdl_client, id_prefix='Z')
    # TODO перенести префикс в конфиг


def get_client_groups(wsdl_client = None, prm_cursor = None, prm_id_list = ''):
    logging.info('Выборка клиентов')
    if prm_id_list == '':
        prm_cursor.execute('''

            select  SP56 as inn, sp4603 as kpp, descr, sp48 as parentname, sp4807 as idartmarket, SP6066 as regionid
            from sc46 where (isfolder<>'1')                       
                           ''')
    else:
        # print(prm_id_list)
        prm_cursor.execute('''
            select  SP56 as inn, sp4603 as kpp,descr,sp48 as parentname,sp4807 as idartmarket,SP6066 as regionid from
             sc46 where (isfolder<>'1') and (sp4807 in (''' + prm_id_list + '''))''')
    logging.info('Выборка клиентов завершена')
    logging.info('Подготовка загрузки клиентов')
    row = prm_cursor.fetchall()
    send_clients(row, wsdl_client=wsdl_client)
