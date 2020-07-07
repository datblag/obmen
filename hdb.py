import logging
from config import filial_region_id, filial_region_name


def unload_maker(cursor=None, wsdl_client=None, objid=''):
    maker_list = []
    hdb_type = wsdl_client.get_type('ns3:hdb_element')
    hdb_array_type = wsdl_client.get_type('ns3:hdb_array_element')
    logging.info('Выборка производители')
    cursor.execute('''
            select SP6120 as idartmarket, SP5470 as inn, SP5471 as kpp, descr  from SC5468
             where id=%s
            ''', objid)
    logging.info('Выборка производители завершена')
    logging.info('Подготовка загрузки производители')
    row = cursor.fetchall()
    for r in row:
        logging.warning(r)
        nom = hdb_type(name=r['descr'].strip(), id=str(r['idartmarket']).strip(), idparent='', value1=r['inn'],
                       value2=r['kpp'])
        maker_list.append(nom)

    hdb_array = hdb_array_type(hdb_array=maker_list)
    logging.info('Загрузка начало производители')
    wsdl_client.service.load_hdb_elements(hdb_array, 1, 'maker')
    logging.info('Загрузка производители завершена')



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
        nom_region=hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(),idparent=r['idparent'])
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
        nom_region = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent='')
        list_region.append(nom_region)

    logging.info('Выборка регионов уровень 2')

    prm_cursor.execute('''select sc46.sp4807 as idartmarket,sc46.descr,scparent.sp4807 as idparent,
                          scparent.descr as nameparent, sc46.id from sc46 left join sc46 as scparent on sc46.parentid=scparent.id
                          where (sc46.parentid in (select id from sc46 where isfolder=1 and ltrim(rtrim(parentid))='0'))
                          and sc46.isfolder=1''')

    logging.info('Выборка регионов уровень 2')
    logging.info('Подготовка регионов уровень 2')
    row = prm_cursor.fetchall()
    for r in row:
        nom_region = hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(), idparent=r['idparent'])
        list_region.append(nom_region)

        logging.info('Выборка регионов уровень 3')
        prm_cursor.execute('''select sc46.sp4807 as idartmarket,sc46.descr,scparent.sp4807 as idparent,
                              scparent.descr as nameparent from sc46 left join sc46 as scparent on sc46.parentid=scparent.id
                              where sc46.isfolder=1 and ltrim(rtrim(sc46.parentid))=%s''', r['id'].strip())
        rows_child = prm_cursor.fetchall()
        logging.info('Подготовка регионов уровень 3')
        for r_child in rows_child:
            nom_region = hdb_type(name=r_child['descr'].strip(), id=r_child['idartmarket'].strip(), idparent=r_child['idparent'])
            list_region.append(nom_region)



    hdb_array = hdb_array_type(hdb_array=list_region)
    logging.info('Загрузка регионов начало')
    wsdl_client.client.service.load_hdb_elements(hdb_array, 1, 'region')
    logging.info('Загрузка регионов завершена')






def send_clients(prm_clients_rows, wsdl_client, id_prefix=''):
    list_clients=[]
    for r in prm_clients_rows:
        inn = '0000000000'
        if not r['inn'].strip() == '':
            inn = r['inn'].strip()

        typett = ''
        if r['typett'] is not None:
            typett = r['typett']

        license1 = ''
        logging.warning(r['license'])
        if r['license'] is not None:
            license1 = r['license']

        logging.warning(r['control_license'])
        control_license = ''
        if r['control_license'] is not None:
            control_license = r['control_license']

        logging.warning([license1.strip(), control_license.strip()])

        nom = wsdl_client.hdb_type(name=r['descr'].strip(), id=r['idartmarket'].strip(),
                                   idparent=r['regionid'].strip(),
                                   value1=id_prefix+inn, value2=r['kpp'].strip(), value3=r['parentname'].strip(),
                                   value4=r['adres'].strip(), value5=r['adres_post'].strip(),
                                   value6=typett.strip(), value7=license1.strip(), value8=control_license.strip(),
                                   value1date=r['license_begin_date'], value2date=r['license_end_date'],
                                   value1num=r['pernodricard_custtype'])
        list_clients.append(nom)

    hdb_array = wsdl_client.hdb_array_type(hdb_array=list_clients)
    logging.info('Загрузка клиентов начало')
    wsdl_client.client.service.load_client_groups(hdb_array, 1)
    logging.info('Загрузка клиентов завершена')



def get_client_groups_filial(wsdl_client = None, prm_cursor = None, prm_id_list = ''):
    logging.info('Выборка клиентов')
    if prm_id_list == '':
        prm_cursor.execute('''
            select  SP9312 as inn, SP9313 as kpp, descr, '' as parentname, SP573 as idartmarket, SP9314 as regionid,
            '' as adres, '' as adres_post, '' as typett
            from SC172 where (isfolder<>'1')                       
                           ''')
    else:
        prm_cursor.execute('''
            select  SP9312 as inn, SP9313 as kpp,descr,'' as parentname,SP573 as idartmarket, SP9314 as regionid,
            '' as adres, '' as adres_post, '' as typett
             from SC172 where (isfolder<>'1') and (SP573 in (''' + prm_id_list + '''))''')
    logging.info('Выборка клиентов завершена')
    logging.info('Подготовка загрузки клиентов')
    row = prm_cursor.fetchall()
    send_clients(row, wsdl_client=wsdl_client, id_prefix='Z')
    # TODO перенести префикс в конфиг


def get_client_groups(wsdl_client=None, prm_cursor=None, prm_id_list='', prm_parent_id_list=None):
    logging.info('Выборка клиентов')
    str_base_sql_query = '''
        select SP56 as inn, sp4603 as kpp, sc46.descr, sp48 as parentname, sp4807 as idartmarket,
        SP6066 as regionid, SP3145 as adres, SP50 as adres_post, SC5464.descr as typett,
        SP3691 as license, SP5239 as control_license, SP5422 as license_begin_date, SP5423 as license_end_date,
        hdb_custtype.code as pernodricard_custtype, hdb_custpositioning.code as pernodricard_custpositioning,
        hdb_custreason.code as  pernodricard_custreason, hdb_custsegment.code as pernodricard_custsegment
        from sc46
        left join SC5464 on  SP5467 = SC5464.id
        left join SC6090 hdb_custtype         on  SP6093 = hdb_custtype.id
        left join SC6094 hdb_custpositioning  on  SP6096 = hdb_custpositioning.id
        left join SC6097 hdb_custreason       on  SP6099 = hdb_custreason.id
        left join SC6100 hdb_custsegment      on  SP6102 = hdb_custsegment.id
        where (isfolder<>'1')  
                           '''
    if prm_id_list == '' and prm_parent_id_list is None:
        prm_cursor.execute(str_base_sql_query)
    elif  prm_parent_id_list is not None:
        prm_cursor.execute(str_base_sql_query + ''' and (parentid=%s)''', prm_parent_id_list['id'])
    else:
        # print(prm_id_list)
        prm_cursor.execute(str_base_sql_query + ''' and (sp4807 in (''' + prm_id_list + '''))''')
    logging.info('Выборка клиентов завершена')
    logging.info('Подготовка загрузки клиентов')
    row = prm_cursor.fetchall()
    send_clients(row, wsdl_client=wsdl_client)
